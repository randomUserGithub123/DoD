use crate::worker::WorkerMessage;
use crate::missing_edge_manager::MissingEdgeManager;
use crate::writer_store::WriterStore;
use crate::execution_threadpool::ExecutionThreadPool;
use petgraph::graphmap::DiGraphMap;
use std::sync::{Arc, Mutex};
use futures::SinkExt;
use tokio::{sync::mpsc, task::JoinHandle};
use std::collections::{LinkedList, HashSet, HashMap, VecDeque};
use bytes::Bytes;
use network::Writer;
use crypto::Digest;
use store::Store;
use smallbank::SmallBankTransactionHandler;
use graph::GlobalOrderGraph;
use log::{error, info};
use petgraph::Direction::Incoming;
use tokio::time::Instant;

type Node = u64;

const MAX_THREADS: usize = 10;

#[derive(Clone)]
struct QueueElement{
    global_order_digest: Digest,
    missed_pairs: HashSet<(Node, Node)>,
    updated_edges: Vec<(Node, Node)>,
}

#[derive(Clone)]
pub struct ExecutionQueue {
    queue: LinkedList<QueueElement>,
    store: Store,
    writer_store: Arc<futures::lock::Mutex<WriterStore>>,
    sb_handler: SmallBankTransactionHandler,
    missed_edge_manager: Arc<futures::lock::Mutex<MissingEdgeManager>>,
    execution_threadpool_size: u32,
}

impl ExecutionQueue {
    pub fn new(store: Store, writer_store: Arc<futures::lock::Mutex<WriterStore>>, sb_handler: SmallBankTransactionHandler, missed_edge_manager: Arc<futures::lock::Mutex<MissingEdgeManager>>, execution_threadpool_size: u32) -> ExecutionQueue {
        ExecutionQueue{
            queue: LinkedList::new(),
            store: store,
            writer_store: writer_store,
            sb_handler: sb_handler,
            missed_edge_manager: missed_edge_manager,
            execution_threadpool_size: execution_threadpool_size
        }
    }

    async fn add_to_queue(&mut self, digest: Digest) {
        info!("TRACE_EQ: add_to_queue START digest={:?}", digest);
        let read_start = Instant::now();
        match self.store.read(digest.to_vec()).await {
            Ok(Some(global_order_info)) => {
                info!("TRACE_EQ: add_to_queue store.read OK in {:?}", read_start.elapsed());
                match bincode::deserialize(&global_order_info).unwrap() {
                    WorkerMessage::GlobalOrderInfo(_global_order_graph, missed) => {
                        info!("TRACE_EQ: add_to_queue deserialized, missed_pairs={}", missed.len());
                        self.queue.push_back(QueueElement{ global_order_digest: digest, missed_pairs: missed, updated_edges: Vec::new()});
                    },
                    _ => panic!("PrimaryWorkerMessage::Execute : Unexpected batch"),
                }
            }
            Ok(None) => {
                info!("TRACE_EQ: add_to_queue store.read returned None for digest={:?} in {:?}", digest, read_start.elapsed());
            },
            Err(e) => error!("TRACE_EQ: add_to_queue store.read error: {} in {:?}", e, read_start.elapsed()),
        }        
    }

    pub async fn execute(&mut self, digest: Digest){
        let execute_start = Instant::now();
        info!("TRACE_EQ: execute START digest={:?}", digest);

        // add new element in the queue associated with this new digest
        self.add_to_queue(digest).await;
        info!("TRACE_EQ: execute after add_to_queue, queue_len={} elapsed={:?}", self.queue.len(), execute_start.elapsed());

        // traverse the queue from front and update missing pairs if any
        let mut queue_idx = 0;
        for element in self.queue.iter_mut() {
            if element.missed_pairs.is_empty(){
                queue_idx += 1;
                continue;
            }

            let mut updated_pairs: Vec<(Node, Node)> = Vec::new();
            let mut updated_edges: Vec<(Node, Node)> = Vec::new();
            let num_missed = element.missed_pairs.len();
            info!("TRACE_EQ: execute checking missed_pairs for queue_idx={} num_missed={}", queue_idx, num_missed);

            for missed_pair in &element.missed_pairs{
                info!("TRACE_EQ: execute locking missed_edge_manager for pair=({}, {})", missed_pair.0, missed_pair.1);
                let lock_start = Instant::now();
                let mut missed_edge_manager_lock = self.missed_edge_manager.lock().await;
                info!("TRACE_EQ: execute got missed_edge_manager lock in {:?}", lock_start.elapsed());

                if let Some(edge) = missed_edge_manager_lock.is_missing_edge_updated(missed_pair.0, missed_pair.1).await {
                    drop(missed_edge_manager_lock);
                    updated_pairs.push((missed_pair.0, missed_pair.1));
                    updated_edges.push((edge.0, edge.1));
                } 
            }

            for pair in &updated_pairs{
                element.missed_pairs.remove(pair);
            }
            for edge in &updated_edges{
                element.updated_edges.push(*edge);
            }
            queue_idx += 1;
        }

        info!("TRACE_EQ: execute done checking missed pairs, elapsed={:?}", execute_start.elapsed());

        // Execute global order if missed edges are found
        let mut n_elements_to_execute = 0;
        for element in self.queue.iter_mut() {
            if element.missed_pairs.is_empty(){
                n_elements_to_execute += 1;
            }
            else{
                break;
            }
        }
        
        info!("TRACE_EQ: execute n_elements_to_execute={} queue_len={}", n_elements_to_execute, self.queue.len());

        // remove queue elements and Execute global order if no more missed edges
        for exec_idx in 0..n_elements_to_execute{
            let queue_element: QueueElement = self.queue.pop_front().unwrap();

            info!("TRACE_EQ: execute element {}/{} reading store for digest={:?}", exec_idx+1, n_elements_to_execute, queue_element.global_order_digest);
            let store_read_start = Instant::now();

            match self.store.read(queue_element.global_order_digest.to_vec()).await {
                Ok(Some(global_order_info)) => {
                    info!("TRACE_EQ: execute store.read OK in {:?}", store_read_start.elapsed());
                    match bincode::deserialize(&global_order_info).unwrap() {
                        WorkerMessage::GlobalOrderInfo(global_order_graph_serialized, _missed) => {
                            let deser_start = Instant::now();
                            let dag: DiGraphMap<Node, u8> = GlobalOrderGraph::get_dag_deserialized(global_order_graph_serialized);
                            
                            log::info!("FINALIZED!: {} (deserialized in {:?})", dag.node_count(), deser_start.elapsed());
                            
                            info!("TRACE_EQ: execute launching ParallelExecution node_count={} edge_count={}", dag.node_count(), dag.edge_count());
                            let par_start = Instant::now();
                            let mut parallel_execution: ParallelExecution = ParallelExecution::new(dag, self.store.clone(), self.writer_store.clone(), self.sb_handler.clone(), self.execution_threadpool_size);
                            parallel_execution.execute().await;
                            info!("TRACE_EQ: execute ParallelExecution DONE in {:?}", par_start.elapsed());
                        },
                        _ => panic!("PrimaryWorkerMessage::Execute : Unexpected global order graph at execution"),
                    }
                }
                Ok(None) => error!("TRACE_EQ: execute global_order_digest NOT FOUND in store"),
                Err(e) => error!("TRACE_EQ: execute store error: {}", e),
            } 
        }

        info!("TRACE_EQ: execute ALL DONE total_elapsed={:?}", execute_start.elapsed());
    }
}



#[derive(Clone)]
pub struct ParallelExecution {
    global_order_graph: DiGraphMap<Node, u8>,
    store: Store,
    writer_store: Arc<futures::lock::Mutex<WriterStore>>,
    sb_handler: SmallBankTransactionHandler,
    execution_threadpool_size: u32,
}

impl ParallelExecution {
    pub fn new(global_order_graph: DiGraphMap<Node, u8>, store: Store, writer_store: Arc<futures::lock::Mutex<WriterStore>>, sb_handler: SmallBankTransactionHandler, execution_threadpool_size: u32) -> ParallelExecution {
        ParallelExecution{
            global_order_graph,
            store,
            writer_store,
            sb_handler,
            execution_threadpool_size,
        }
    }

    pub fn schedule_node(tx_uid: u64, writer_store: Arc<futures::lock::Mutex<WriterStore>>, tx_done: mpsc::UnboundedSender<u64>) {
                
        tokio::spawn(async move {
            let tx_id_vec = tx_uid.to_be_bytes().to_vec();
            {
                let mut writer_store_lock = writer_store.lock().await;
                if writer_store_lock.writer_exists(tx_uid){
                    let mut writer: Arc<futures::lock::Mutex<Writer>> = writer_store_lock.get_writer(tx_uid);
                    let mut writer_lock = writer.lock().await;
                    let _ = writer_lock.send(Bytes::from(tx_id_vec)).await;
                    writer_store_lock.delete_writer(tx_uid);
                }
            }
            let _ = tx_done.send(tx_uid);
        });
    }

    pub async fn execute(&mut self) {
        let exec_start = Instant::now();
        let total_nodes = self.global_order_graph.node_count();
        let total_edges = self.global_order_graph.edge_count();
        let mut scheduled_count = 0;
    
        info!("TRACE_PE: execute START total_nodes={} total_edges={} threadpool_size={}", total_nodes, total_edges, self.execution_threadpool_size);

        let mut in_degree_map: HashMap<Node, usize> = HashMap::new();
        for node in self.global_order_graph.nodes() {
            in_degree_map.insert(node, self.global_order_graph.edges_directed(node, Incoming).count());
        }

        let zero_indeg_count = in_degree_map.values().filter(|&&d| d == 0).count();
        info!("TRACE_PE: execute in_degree_map built, zero_indeg_nodes={}", zero_indeg_count);

        let (tx_done, mut rx_done) = mpsc::unbounded_channel::<u64>();
        let tx_done_clone = tx_done.clone();
    
        let thread_pool_size = self.execution_threadpool_size as usize;
        let store_clone = self.store.clone();
        let writer_store_clone = self.writer_store.clone();
        let sb_handler_clone = self.sb_handler.clone(); 
    
        info!("TRACE_PE: execute creating thread pool size={}", thread_pool_size);
        let pool_start = Instant::now();
        let thread_pool = ExecutionThreadPool::new(
            thread_pool_size,
            store_clone,
            writer_store_clone,
            sb_handler_clone,
            tx_done,
        );
        info!("TRACE_PE: execute thread pool created in {:?}", pool_start.elapsed());

        // Spawn tasks for nodes with in-degree 0
        {
            for (node, in_degree) in &in_degree_map {
                if *in_degree == 0 {
                    scheduled_count += 1;
                    thread_pool.send_message(*node).await;
                }
            }
        }
        info!("TRACE_PE: execute scheduled {} initial zero-indeg nodes", scheduled_count);

        if scheduled_count == 0 && total_nodes > 0 {
            error!("TRACE_PE: BUG! No zero-indegree nodes but graph has {} nodes — possible cycle!", total_nodes);
        }

        let mut completed_count = 0;
        loop {
            info!("TRACE_PE: execute waiting on rx_done completed={}/{} scheduled={}", completed_count, total_nodes, scheduled_count);
            let recv_start = Instant::now();

            let completed_id = match rx_done.recv().await {
                Some(id) => {
                    info!("TRACE_PE: execute rx_done received tx_uid={} wait_time={:?}", id, recv_start.elapsed());
                    id
                },
                None => {
                    error!("TRACE_PE: execute rx_done channel CLOSED unexpectedly! completed={}/{}", completed_count, total_nodes);
                    break;
                }
            };

            completed_count += 1;
            if completed_count == total_nodes {
                info!("TRACE_PE: execute all {} nodes completed", total_nodes);
                break;
            }
    
            let mut flag = false;
            if completed_count == scheduled_count {
                flag = true;
                info!("TRACE_PE: execute caught_up: completed==scheduled=={}", completed_count);
            }
    
            let mut newly_scheduled = 0;
            for neighbor in self.global_order_graph.neighbors(completed_id) {
                in_degree_map.entry(neighbor).and_modify(|degree| *degree -= 1);
                if *in_degree_map.get(&neighbor).unwrap() == 0 {
                    scheduled_count += 1;
                    newly_scheduled += 1;
                    thread_pool.send_message(neighbor).await;
                }
            }
            if newly_scheduled > 0 {
                info!("TRACE_PE: execute after tx_uid={}: newly_scheduled={} total_scheduled={}", completed_id, newly_scheduled, scheduled_count);
            }

            if flag && completed_count == scheduled_count {
                info!("TRACE_PE: execute EARLY EXIT: no more work, completed={} scheduled={} total_nodes={}", completed_count, scheduled_count, total_nodes);
                break;
            }
        }
    
        info!("TRACE_PE: execute shutting down thread pool, completed={}/{} elapsed={:?}", completed_count, total_nodes, exec_start.elapsed());
        let shutdown_start = Instant::now();
        thread_pool.shutdown().await;
        info!("TRACE_PE: execute thread pool shutdown DONE in {:?}, total_elapsed={:?}", shutdown_start.elapsed(), exec_start.elapsed());
    }   
}

#[derive(Clone)]
pub struct ParallelExecutionThread {
    global_order_graph: DiGraphMap<Node, u8>,
    store: Store,
    writer_store: Arc<futures::lock::Mutex<WriterStore>>,
    sb_handler: SmallBankTransactionHandler,
    shared_queue: Arc<Mutex<VecDeque<Node>>>,
}

impl ParallelExecutionThread {

    pub fn spawn(
        global_order_graph: DiGraphMap<Node, u8>,
        store: Store,
        writer_store: Arc<futures::lock::Mutex<WriterStore>>,
        sb_handler: SmallBankTransactionHandler,
        shared_queue: Arc<Mutex<VecDeque<Node>>>,
    ) -> JoinHandle<()> {
        let task = tokio::spawn(async move {
            Self {
                global_order_graph,
                store,
                writer_store,
                sb_handler,
                shared_queue,
            }
            .run()
            .await;
        });

        return task;
    }

    async fn run(&mut self) {
        loop {
            let mut tx_uid: u64;
            {
                let mut locked_queue = self.shared_queue.lock().unwrap();
                if locked_queue.is_empty() { 
                    break; 
                }

                tx_uid = locked_queue.pop_front().unwrap();
            }
            let tx_id_vec = tx_uid.to_be_bytes().to_vec();

            match self.store.read(tx_id_vec.clone()).await {
                Ok(Some(tx)) => {
                    self.sb_handler.execute_transaction(Bytes::from(tx));
                    {
                        let mut writer_store_lock = self.writer_store.lock().await;
                        if writer_store_lock.writer_exists(tx_uid){
                            let mut writer: Arc<futures::lock::Mutex<Writer>> = writer_store_lock.get_writer(tx_uid);
                            drop(writer_store_lock);
                            let mut writer_lock = writer.lock().await;
                            let _ = writer_lock.send(Bytes::from(tx_id_vec)).await;
                        }
                    }                        
                }
                Ok(None) => error!("ParallelExecutionThread :: Cannot find tx_uid = {:?} in the store", tx_uid),
                Err(e) => error!("{}", e),
            } 
            for neighbor in self.global_order_graph.neighbors(tx_uid){
                self.shared_queue.lock().unwrap().push_back(neighbor);
            }
        }
    }
}
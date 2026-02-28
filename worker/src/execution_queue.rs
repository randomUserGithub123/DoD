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
        match self.store.read(digest.to_vec()).await {
            Ok(Some(global_order_info)) => {
                match bincode::deserialize(&global_order_info).unwrap() {
                    WorkerMessage::GlobalOrderInfo(_global_order_graph, missed) => {
                        self.queue.push_back(QueueElement{ global_order_digest: digest, missed_pairs: missed, updated_edges: Vec::new()});
                    },
                    _ => panic!("PrimaryWorkerMessage::Execute : Unexpected batch"),
                }
            }
            Ok(None) => {},
            Err(e) => error!("error while adding a digest to the execution queue = {}", e),
        }        
    }

    pub async fn execute(&mut self, digest: Digest){
        info!("execution STARTS");
        self.add_to_queue(digest).await;
        info!("execution queue length = {:?}", self.queue.len());

        // traverse the queue from front and update missing pairs if any
        for element in self.queue.iter_mut() {
            if element.missed_pairs.is_empty(){
                continue;
            }

            let mut updated_pairs: Vec<(Node, Node)> = Vec::new();
            let mut updated_edges: Vec<(Node, Node)> = Vec::new();
            for missed_pair in &element.missed_pairs{
                info!("ExecutionQueue::execute missed pair = {:?}", missed_pair);
                let mut missed_edge_manager_lock = self.missed_edge_manager.lock().await;
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
        }

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
        
        info!("n_elements_to_execute = {:?}", n_elements_to_execute);

        for _ in 0..n_elements_to_execute{
            let queue_element: QueueElement = self.queue.pop_front().unwrap();

            match self.store.read(queue_element.global_order_digest.to_vec()).await {
                Ok(Some(global_order_info)) => {
                    match bincode::deserialize(&global_order_info).unwrap() {
                        WorkerMessage::GlobalOrderInfo(global_order_graph_serialized, _missed) => {
                            let dag: DiGraphMap<Node, u8> = GlobalOrderGraph::get_dag_deserialized(global_order_graph_serialized);
                            
                            log::info!("FINALIZED!: {}", dag.node_count());
                            
                            let mut parallel_execution: ParallelExecution = ParallelExecution::new(dag, self.store.clone(), self.writer_store.clone(), self.sb_handler.clone(), self.execution_threadpool_size);
                            parallel_execution.execute().await;    
                        },
                        _ => panic!("PrimaryWorkerMessage::Execute : Unexpected global order graph at execution"),
                    }
                }
                Ok(None) => error!("ExecutionQueue:execute :: global_order_digest not found in the store"),
                Err(e) => error!("{}", e),
            } 
        }
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
        let total_nodes = self.global_order_graph.node_count();
        if total_nodes == 0 {
            info!("ParallelExecution: empty graph, skipping");
            return;
        }

        let mut in_degree_map: HashMap<Node, usize> = HashMap::new();
        let mut zero_indegree_count: usize = 0;
        for node in self.global_order_graph.nodes() {
            let indeg = self.global_order_graph.edges_directed(node, Incoming).count();
            if indeg == 0 {
                zero_indegree_count += 1;
            }
            in_degree_map.insert(node, indeg);
        }

        info!(
            "ParallelExecution: total_nodes={}, zero_indegree={}, edge_count={}, pool_size={}",
            total_nodes,
            zero_indegree_count,
            self.global_order_graph.edge_count(),
            self.execution_threadpool_size
        );

        if zero_indegree_count == 0 {
            error!("ParallelExecution: NO zero-indegree nodes — graph has a cycle, aborting");
            return;
        }

        let (tx_done, mut rx_done) = mpsc::unbounded_channel::<u64>();
    
        let thread_pool_size = self.execution_threadpool_size as usize;
        let store_clone = self.store.clone();
        let writer_store_clone = self.writer_store.clone();
        let sb_handler_clone = self.sb_handler.clone(); 
    
        let thread_pool = ExecutionThreadPool::new(
            thread_pool_size,
            store_clone,
            writer_store_clone,
            sb_handler_clone,
            tx_done,
        );
    
        // Schedule all zero in-degree nodes
        let mut scheduled_zero = 0;
        for (node, in_degree) in &in_degree_map {
            if *in_degree == 0 {
                thread_pool.send_message(*node).await;
                scheduled_zero += 1;
            }
        }

        info!(
            "ParallelExecution: scheduled {} zero-indegree nodes, entering completion loop",
            scheduled_zero
        );

        let loop_start = std::time::Instant::now();
        let mut completed_count: usize = 0;
        let mut last_log_time = std::time::Instant::now();

        while let Some(completed_id) = rx_done.recv().await {
            completed_count += 1;

            // Log progress periodically: every 500 completions or every 5 seconds
            let now = std::time::Instant::now();
            if completed_count % 500 == 0 || now.duration_since(last_log_time).as_secs() >= 5 {
                info!(
                    "ParallelExecution: completed {}/{} (elapsed {}ms)",
                    completed_count,
                    total_nodes,
                    loop_start.elapsed().as_millis()
                );
                last_log_time = now;
            }

            if completed_count == total_nodes {
                break;
            }

            // Decrement in-degree for neighbors and schedule them if their in-degree reaches 0
            for neighbor in self.global_order_graph.neighbors(completed_id) {
                if let Some(degree) = in_degree_map.get_mut(&neighbor) {
                    *degree -= 1;
                    if *degree == 0 {
                        thread_pool.send_message(neighbor).await;
                    }
                }
            }
        }

        if completed_count < total_nodes {
            // Log which nodes are stuck
            let stuck_nodes: Vec<(Node, usize)> = in_degree_map.iter()
                .filter(|(_, deg)| **deg > 0)
                .map(|(n, d)| (*n, *d))
                .take(20)  // limit output
                .collect();
            error!(
                "ParallelExecution: only completed {}/{} nodes (elapsed {}ms). Stuck nodes (first 20): {:?}",
                completed_count,
                total_nodes,
                loop_start.elapsed().as_millis(),
                stuck_nodes
            );
        } else {
            info!(
                "ParallelExecution: all {}/{} nodes completed in {}ms",
                completed_count,
                total_nodes,
                loop_start.elapsed().as_millis()
            );
        }

        thread_pool.shutdown().await;
        info!("ParallelExecution: shutdown complete");
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
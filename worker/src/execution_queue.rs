
use crate::worker::WorkerMessage;
use crate::missing_edge_manager::MissingEdgeManager;
use crate::writer_store::WriterStore;
use petgraph::graphmap::DiGraphMap;
use std::sync::{Arc, Mutex};
use futures::SinkExt;
use tokio::task::JoinHandle;
use std::collections::{LinkedList, HashSet, HashMap, VecDeque};
use bytes::Bytes;
use network::Writer;
use crypto::Digest;
use store::Store;
use smallbank::SmallBankTransactionHandler;
use graph::GlobalOrderGraph;
use log::{error, info};

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
}

impl ExecutionQueue {
    pub fn new(store: Store, writer_store: Arc<futures::lock::Mutex<WriterStore>>, sb_handler: SmallBankTransactionHandler, missed_edge_manager: Arc<futures::lock::Mutex<MissingEdgeManager>>) -> ExecutionQueue {
        ExecutionQueue{
            queue: LinkedList::new(),
            store: store,
            writer_store: writer_store,
            sb_handler: sb_handler,
            missed_edge_manager: missed_edge_manager,
        }
    }

    async fn add_to_queue(&mut self, digest: Digest) {
        match self.store.read(digest.to_vec()).await {
            Ok(Some(global_order_info)) => {
                match bincode::deserialize(&global_order_info).unwrap() {
                    WorkerMessage::GlobalOrderInfo(_global_order_graph, missed) => {
                        info!("Adding digest = {:?} to the execution queue", digest);
                        self.queue.push_back(QueueElement{ global_order_digest: digest, missed_pairs: missed, updated_edges: Vec::new()});
                    },
                    _ => panic!("PrimaryWorkerMessage::Execute : Unexpected batch"),
                }
            }
            Ok(None) => error!("Could not find a digest in the store while adding to the execution queue"),
            Err(e) => error!("error while adding a digest to the execution queue = {}", e),
        }        
    }

    pub async fn execute(&mut self, digest: Digest){
        // add new element in the queue associated with this new digest
        info!("execution STARTS");
        self.add_to_queue(digest).await;
        info!("execution queue length = {:?}", self.queue.len());

        // traverse the queue from front and update missing pairs if any
        for element in self.queue.iter_mut() {
            info!("New element to check for missed pairs");
            // check if missed edges are found for this digest
            if element.missed_pairs.is_empty(){
                info!("No missed pairs");
                continue;
            }

            let mut updated_pairs: Vec<(Node, Node)> = Vec::new();
            let mut updated_edges: Vec<(Node, Node)> = Vec::new();
            for missed_pair in &element.missed_pairs{
                info!("missed pair = {:?}", missed_pair);
                let mut missed_edge_manager_lock = self.missed_edge_manager.lock().await;
                if missed_edge_manager_lock.is_missing_edge_updated(missed_pair.0, missed_pair.1).await{
                    info!("missed edge found");
                    updated_pairs.push((missed_pair.0, missed_pair.1));
                    updated_edges.push((missed_pair.0, missed_pair.1));
                }
                else if missed_edge_manager_lock.is_missing_edge_updated(missed_pair.1, missed_pair.0).await{
                    info!("missed edge found");
                    updated_pairs.push((missed_pair.0, missed_pair.1));
                    updated_edges.push((missed_pair.1, missed_pair.0));
                }
            }

            // remove from missed pairs set
            for pair in &updated_pairs{
                element.missed_pairs.remove(pair);
            }
            // add to the updated edges
            for edge in &updated_edges{
                element.updated_edges.push(*edge);
            }
        }

        // Execute global order if missed edges are found
        let mut n_elements_to_execute = 0;
        for element in self.queue.iter_mut() {
            // check if there are no missed edges for this digest
            if element.missed_pairs.is_empty(){
                // TODO: Update the graph based on the "updated_edges"
                n_elements_to_execute += 1;
            }
            else{
                // execution can only be done in sequence
                break;
            }
        }

        info!("n_elements_to_execute = {:?}", n_elements_to_execute);

        // remove queue elements and Execute global order if no more missed edges
        for _ in 0..n_elements_to_execute{
            let queue_element: QueueElement = self.queue.pop_front().unwrap();

            // execute the global order graph
            match self.store.read(queue_element.global_order_digest.to_vec()).await {
                Ok(Some(global_order_info)) => {
                    match bincode::deserialize(&global_order_info).unwrap() {
                        WorkerMessage::GlobalOrderInfo(global_order_graph_serialized, _missed) => {
                            // deserialize received serialized glbal order graph
                            let dag: DiGraphMap<Node, u8> = GlobalOrderGraph::get_dag_deserialized(global_order_graph_serialized);
                            info!("Sending graph to the parallel execution");
                            let mut parallel_execution:  ParallelExecution = ParallelExecution::new(dag, self.store.clone(), self.writer_store.clone(), self.sb_handler.clone());
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
}

impl ParallelExecution {
    pub fn new(global_order_graph: DiGraphMap<Node, u8>, store: Store, writer_store: Arc<futures::lock::Mutex<WriterStore>>, sb_handler: SmallBankTransactionHandler) -> ParallelExecution {
        ParallelExecution{
            global_order_graph,
            store,
            writer_store,
            sb_handler,
        }
    }

    pub async fn execute(&mut self){
        // find incoming edge count for each node in the graph
        info!("ParallelExecution:execute");
        info!("ParallelExecution:execute :: #nodes in graph = {:?}", self.global_order_graph.node_count());

        // Create a HashMap to store in-degrees of nodes
        // let in_degrees: Arc<futures::lock::Mutex<HashMap<Node, usize>>> = Arc::new(Mutex::new(HashMap::new()));
        // in_degrees_unlocked = in_degrees.lock().await;
        // for node in self.global_order_graph.nodes() {
        //     in_degrees_unlocked.insert(node, self.global_order_graph.edges_directed(node, Incoming).count());
        // }

        // // Create a queue for nodes with in-degree 0
        // let mut queue = VecDeque::new();

        // // Initialize the queue with nodes that have in-degree 0
        // for (&node, &degree) in in_degrees.iter() {
        //     if degree == 0 {
        //         queue.push_back(node);
        //     }
        // }

        // Comment out the old implementation
        
        // TEST: START
        for tx_uid in self.global_order_graph.nodes(){
            info!("ParallelExecution::execute : tx_uid = {:?} is going to execute in", tx_uid);
            let tx_id_vec = tx_uid.to_be_bytes().to_vec();
            {
                let mut writer_store_lock = self.writer_store.lock().await;
                if writer_store_lock.writer_exists(tx_uid){
                    info!("ParallelExecution::execute : tx_uid = {:?} does exist in writer store", tx_uid);
                    let mut writer: Arc<futures::lock::Mutex<Writer>> = writer_store_lock.get_writer(tx_uid);
                    // drop(writer_store_lock);
                    let mut writer_lock = writer.lock().await;
                    let _ = writer_lock.send(Bytes::from(tx_id_vec)).await;
                    writer_store_lock.delete_writer(tx_uid);
                }
                else{
                    info!("ParallelExecution::execute : tx_uid = {:?} does not exist in writer store", tx_uid);
                }
            }
        }
        // info!("ParallelExecution::execute : Test ends");
        // TEST: END



        // let mut incoming_count: HashMap<Node, usize> = HashMap::new();
        // for node in self.global_order_graph.nodes(){
        //     incoming_count.entry(node).or_insert(0);
        //     for neighbor in self.global_order_graph.neighbors(node){
        //         incoming_count.entry(neighbor).or_insert(0);
        //         incoming_count.insert(neighbor, incoming_count.get(&neighbor).unwrap()+1);
        //     }                   
        // }

        // // find root nodes of the graph
        // let mut roots: Vec<Node> = Vec::new();
        // for (node, count) in &incoming_count{
        //     if *count==0{
        //         roots.push(*node);
        //     }
        // }
        // info!("ParallelExecution:execute :: #roots found = {:?}", roots.len());
        // // create a shared queue: https://stackoverflow.com/questions/72879440/how-to-use-vecdeque-in-multi-threaded-app
        // let shared_queue = Arc::new(Mutex::new(VecDeque::new()));
        
        // // initialize the shared queue with root nodes
        // for root in roots{
        //     shared_queue.lock().unwrap().push_back(root);
        // }

        // // Traverse the graph and execute the nodes using thread pool
        // let mut blocking_tasks: Vec<JoinHandle<()>> = Vec::new();
        // for _ in 0..MAX_THREADS {
        //     let task = ParallelExecutionThread::spawn(self.global_order_graph.clone(), self.store.clone(), self.writer_store.clone() ,self.sb_handler.clone(), shared_queue.clone());
        //     blocking_tasks.push(task);
        // }

        // // joining all the threads
        // for task in blocking_tasks{
        //     let _ = task.await;
        // }
        
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
            info!("tx_uid = {:?} is going to execute in ParallelExecutionThread", tx_uid);
            let tx_id_vec = tx_uid.to_be_bytes().to_vec();

            // Get the actual transaction against tx_id from the Store
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
            // Add neighbors of the current node
            for neighbor in self.global_order_graph.neighbors(tx_uid){
                self.shared_queue.lock().unwrap().push_back(neighbor);
            }
        }
    }
}
use rand::Rng;
use std::cell::RefCell;
use std::rc::Rc;

const MAX_LEVEL: usize = 8;

type Link = Option<Rc<RefCell<Node>>>;
//Node struct of a skip list
#[derive(Debug)]
struct Node {
    id: i32,
    #[allow(dead_code)]
    payload: String,
    fwd: [Link; MAX_LEVEL], // fixed array for skip list levels
}
//implemedntation of {Node}/
impl Node {
    //function to create a new node
    fn new(id: i32, payload: &str) -> Self {
        Node {
            id,
            payload: payload.to_string(),
            fwd: Default::default(), // all None
        }
    }
    fn get_lvlsize(&self)->usize{
        return MAX_LEVEL;
    }
}
//SkipList struct
struct SkipList {
    head: Rc<RefCell<Node>>,
    p: i32,
    lvl_count: [usize; MAX_LEVEL]
}
//implementation of SkipList
impl SkipList {
    //function to createa new head with prob(p) as main distibutor
    fn new(p: i32) -> Self {
        SkipList {
            head: Rc::new(RefCell::new(Node::new(-1, ""))),
            p,
            lvl_count: [0; MAX_LEVEL],
        }
    }
    //function to generate random level for node insertion
    fn gen_random_level(&self) -> usize {
        let mut lvl = 0;
        let mut rng = rand::rng();
        while rng.random_range(0..100) < self.p && lvl < MAX_LEVEL - 1 {
            lvl += 1;
        }
        lvl
    }

    //TODO: function to self balance the skip list before insertion


    //function to insert a new node in the skip list
    //has id key and payload as value
    //loop through the key and arr and while loop through the levels to find empty forward pointer
    fn insert(&mut self, id: i32, payload: &str) {
        let lvl = self.gen_random_level();
        let new_node = Rc::new(RefCell::new(Node::new(id, payload)));

        for i in (0..=lvl).rev() {
            let mut current = Rc::clone(&self.head);
            loop {
                // clone the next pointer first
                let next_opt = current.borrow().fwd[i].as_ref().map(Rc::clone);

                match next_opt {
                    Some(next) if next.borrow().id < id => {
                        current = next; // safe: borrow already dropped
                    }
                    _ => break,
                }
            }

            // insert new_node between current and current.fwd[i]
            new_node.borrow_mut().fwd[i] = current.borrow_mut().fwd[i].take();
            current.borrow_mut().fwd[i] = Some(Rc::clone(&new_node));
            self.lvl_count[i] += 1;
        }
    }
    //function to search
fn search(&self, id: i32) -> Option<String> {
    let mut current = Rc::clone(&self.head);

    // Start from the highest possible level down to 0
    for i in (0..MAX_LEVEL).rev() {
        loop {
            let next_opt = current.borrow().fwd[i].as_ref().map(Rc::clone);

            match next_opt {
                Some(next) if next.borrow().id < id => {
                    current = next; // keep moving right
                }
                _ => break, // drop down one level
            }
        }
    }

    // After descending, move to the candidate node
    if let Some(next) = current.borrow().fwd[0].as_ref().map(Rc::clone) {
        if next.borrow().id == id {
            return Some(next.borrow().payload.clone());
        }
    }

    None
}

    fn print_list(&self) {
        for i in (0..MAX_LEVEL).rev() {
            let mut node_opt = self.head.borrow().fwd[i].as_ref().map(Rc::clone);
            print!("Level {}: ", i);
            while let Some(node) = node_opt {
                print!("{} -> ", node.borrow().id);
                node_opt = node.borrow().fwd[i].as_ref().map(Rc::clone);
            }
            println!("None");
        }
    }
}

fn main() {
    let mut sl = SkipList::new(50);
    sl.insert(10, "ten");
    sl.insert(5, "five");
    sl.insert(20, "twenty");
    sl.insert(15, "fifteen");
    sl.print_list();
    println!("{:?}", sl.search(5))
}

use std::rc::Rc;
use std::cell::RefCell;
use rand::Rng;

const MAX_LEVEL: usize = 16;

#[derive(Debug)]
struct Node {
    id: i32,
    payload: String,
    fwd: [Option<Rc<RefCell<Node>>>; MAX_LEVEL],
}

impl Node {
    fn new(id: i32, payload: String) -> Self {
        Node {
            id,
            payload,
            fwd: Default::default(),
        }
    }
}

struct SkipList {
    max_level: usize,
    p: i32,
    head: Rc<RefCell<Node>>,
}

impl SkipList {
    fn new(max_level: usize, p: i32) -> Self {
        SkipList {
            max_level,
            p,
            head: Rc::new(RefCell::new(Node::new(-1, String::new()))),
        }
    }

    fn gen_random_level(&self) -> usize {
        let mut lvl = 0;
        let mut rng = rand::rng();
        while rng.random_range(0..100) < self.p && lvl < self.max_level - 1 {
            lvl += 1;
        }
        lvl
    }

    fn insert(&mut self, id: i32, payload: String) {
        let lvl = self.gen_random_level();
        let new_node = Rc::new(
            RefCell::new(Node::new(id, payload))
        );
        for i in 0..=lvl {
            let mut head = self.head.borrow_mut();
            if let Some(next) = head.fwd[i].take() {
                new_node.borrow_mut().fwd[i] = Some(next);
            }
            head.fwd[i] = Some(Rc::clone(&new_node));
        }
    }
}

fn main() {
    let mut sl = SkipList::new(MAX_LEVEL, 50);
    sl.insert(1, "hello".to_string());
    println!("{:#?}", sl.head);
}
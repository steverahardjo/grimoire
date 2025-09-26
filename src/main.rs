use rand::Rng;

#[derive(Debug)]
struct Node {
    id: i32,
    payload: String,
    forward: Vec<Option<Box<Node>>>,
}

impl Node {
    fn new(id: i32, payload: String, level: usize) -> Self {
        Node {
            id,
            payload,
            forward: vec![None; level],
        }
    }
}


struct SkipList {
    max_level: usize,
    p: i32,
    head: Box<Node>,
}

impl SkipList {
    fn new(max_level: usize, p: i32) -> Self {
        SkipList {
            max_level,
            p,
            head: Box::new(Node::new(-1, String::new(), max_level)),
        }
    }

    fn gen_random_level(&self) -> usize {
        let mut lvl = 1;
        while rand::rng().gen_range(0..100) < self.p && lvl < self.max_level {
            lvl += 1;
        }
        lvl
    }

    fn insert(&mut self, id: i32, payload: String) {
        let lvl = self.gen_random_level();
        let new_node = Box::new(Node::new(id, payload, lvl));
        // TODO: update forward pointers
        println!("Generated node with level {}", lvl);
    }
}

fn main() {
    let mut sl = SkipList::new(8, 50);
    sl.insert(1, "hello".to_string());
}

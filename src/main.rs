//Implement a skip list

use rand::Rng;
use std::fmt::Debug;
use std::cell::RefCell;


// Create a struct Node{id, payload, arr[]}
fn main() {
    println!("Hello, world!");
}

struct SkipList{
    max_level:i32,
    p:132,
    starter:Node
}

impl SkipList{
    fn get_max_level(&self)->i32{
        self.max_level
    }
    fn add_node(&self, id:i32, payload:string)->Option<&Node>{
    }
    fn gen_random_level(&self)->i32{
        let lvl:i32 = 1
        while rand::thread_rng().gen_range(0..100) < self.p && lvl<self.max_level{
            level+=1
        }
        return lvl
    }
}
#[derive(Clone, Debug)]
struct Node<T: Eq + >{
    id:i32,
    payload:string,
    fwd: []
}

impl Node{
    fn add(&self, id:i32, payload:string)->Option<&Node>{
        let lvl:132 = self.gen_random_level()
        i   
    }
}

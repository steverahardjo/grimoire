//Implement a skip list

use rand::Rng;
use std::fmt::Debug;
use std::cell::RefCell;


// Create a struct Node{id, payload, arr[]}
fn main() {
    println!("Hello, world!");
}
#[derive(Clone, Debug)]
struct Node<T: Eq + >{
    id:i32,
    payload:string,
    fwd: []
}

impl Node{
    fn gen_random_level(&self, p:i32, max_level:i32)->i32{
        let lvl:i32 = 1
        while rand::thread_rng().gen_range(0..100) < p && lvl<max_level{
            level+=1
        }
        return lvl
    }
    fn add(&self, id:i32, payload:string)->Option<&Node>{
        let lvl:132 = self.gen_random_level()
        i   
    }
}

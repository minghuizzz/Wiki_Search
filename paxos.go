package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (decided bool, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import "net"
import "net/rpc"
import "log"
import "os"
import "syscall"
import "sync"
import "fmt"
import "math/rand"
import "time"
import "bytes"
import "strconv"
import "runtime"


type Paxos struct {
  mu sync.Mutex
  l net.Listener
  dead bool
  unreliable bool
  rpcCount int
  peers []string
  me int // index into peers[]


  // Your data here.
  instances map[int]Instance
  donevals map[int]int//done values of peers
}

func (px *Paxos) Log() {
  //px.mu.Lock()
  // fmt.Println(px.peers)
  log.Println("me:", px.me, "instance:", px.instances, "donevals:", px.donevals)
  //fmt.Println(px.instances)
  //fmt.Println(px.donevals)
  //px.mu.Unlock()
}

func Log2() {
	fmt.Println()
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
  c, err := rpc.Dial("unix", srv)
  if err != nil {
  	err1 := err.(*net.OpError)
    if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
      //fmt.Printf("paxos Dial() failed: %v\n", err1)
    }
    return false
  }
  defer c.Close()


  err = c.Call(name, args, reply)
  if err == nil {
    return true
  }

  fmt.Println(err)
  return false
}


func (px *Paxos) Prepare(seq int, pid int) (bool, interface{}) {
  px.mu.Lock()
  ins, exist := px.instances[seq]
  px.mu.Unlock()
  if !exist {
    ins = Instance{}
    ins.Seq = seq
    ins.N_P = -1
    ins.N_A = -1
  }
  numok := 0
  if pid > ins.N_P {
    numok = 1
    ins.N_P = pid
    px.mu.Lock()
    px.instances[seq] = ins
    px.mu.Unlock()
  }

  args := &PrepareArgs{}
  args.Seq = ins.Seq
  args.PID = pid
  args.Me = px.me
  px.mu.Lock()
  args.Doneval = px.donevals[px.me]
  px.mu.Unlock()
  half := len(px.peers) / 2
  maxn_a := -1
  maxre := &PrepareReply{}


  for i := range px.peers {
    if i != px.me {
      reply := &PrepareReply{}
      ok := false
      for j := 0; j < 10 && !ok; j++ {
        ok = call(px.peers[i], "Paxos.PrepareHandler", args, reply)
        if reply.N_P > ins.N_P {
          ins.N_P = reply.N_P
          px.mu.Lock()
          px.instances[seq] = ins
          px.mu.Unlock()
        }
      }
      if ok {
        if reply.OK {
          numok++
          if reply.N_A > maxn_a {
            maxn_a = reply.N_A
            maxre = reply
          }
        } else {
          if reply.V_A != nil {
            px.Decided(seq, reply.V_A)
            break
            // ins.Val = reply.V_A
            // px.mu.Lock()
            // px.instances[seq] = ins
            // px.mu.Unlock()
          }
        }
        //px.Done(reply.Doneval)
        px.mu.Lock()
        px.donevals[i] = reply.Doneval
        px.mu.Unlock()

      }
    }

  }
  px.Done(args.Doneval)
  if ins.N_A > maxn_a {
    return numok > half, ins.V_A
  }
  return numok > half, maxre.V_A
}

func (px *Paxos) PrepareHandler(args *PrepareArgs, reply *PrepareReply) error {
  //px.Done(args.Doneval)
  px.mu.Lock()
  px.donevals[args.Me] = args.Doneval
  reply.Doneval = px.donevals[px.me]
  px.mu.Unlock()

  px.Done(reply.Doneval)

  px.mu.Lock()
  ins, exist := px.instances[args.Seq]
  px.mu.Unlock()
  if !exist {
    ins = Instance{}
    ins.Seq = args.Seq
    ins.N_P = -1
    ins.N_A = -1
  }
  if ins.Val != nil {
    reply.V_A = ins.Val
    reply.OK = false
    reply.N_P = ins.N_P
    return nil
  }

  if ins.N_P < args.PID {//|| (ins.N_P == args.PID && ins.Val == nil)
    ins.N_P = args.PID
    reply.OK = true
    reply.N_A = ins.N_A
    reply.V_A = ins.V_A
    px.mu.Lock()
    px.instances[args.Seq] = ins
    px.mu.Unlock()
  } else {
    reply.OK = false
    reply.N_P = ins.N_P
  }


  return nil
}

func (px *Paxos) Accept(seq int, pid int, v interface{}) bool {
  px.mu.Lock()
  ins, exist := px.instances[seq]
  px.mu.Unlock()

  if !exist {
    ins = Instance{}
    ins.Seq = seq
    ins.N_P = -1
    ins.N_A = -1
  }

  numok := 0
  if pid >= ins.N_P {
    numok = 1
    ins.N_A = pid
    ins.V_A = v
    px.mu.Lock()
    px.instances[seq] = ins
    px.mu.Unlock()
  }

  args := &AcceptArgs{}
  args.Seq = ins.Seq
  args.PID = pid
  args.Val = v
  args.Me = px.me
  px.mu.Lock()
  args.Doneval = px.donevals[px.me]
  px.mu.Unlock()
  half := len(px.peers) / 2


  for i := range px.peers {
    if i != px.me {
      reply := & AcceptReply{}
      ok := false
      for j := 0; j < 10 && !ok; j++ {
        ok = call(px.peers[i], "Paxos.AcceptHandler", args, reply)
      }
      if ok {
        if reply.OK {
          numok++
        } else {
          if reply.V_D != nil {
            //ins.Val = reply.V_D
            px.Decided(seq, reply.V_D)
            break
            // px.mu.Lock()
            // px.instances[seq] = ins
            // px.mu.Unlock()

          }
        }
        px.mu.Lock()
        px.donevals[i] = reply.Doneval
        px.mu.Unlock()

      }
    }
  }
  px.Done(args.Doneval)

  return numok > half

}

func (px *Paxos) AcceptHandler(args *AcceptArgs, reply *AcceptReply) error {
  //px.Done(args.Doneval)

  px.mu.Lock()
  px.donevals[args.Me] = args.Doneval
  reply.Doneval = px.donevals[px.me]
  px.mu.Unlock()

  px.Done(reply.Doneval)

  px.mu.Lock()
  ins, _ := px.instances[args.Seq]
  px.mu.Unlock()
  if ins.Val != nil {
    reply.V_D = ins.Val
    reply.OK = false
    return nil
  }
  if args.PID >= ins.N_P {
    ins.N_P = args.PID
    ins.N_A = args.PID
    ins.V_A = args.Val
    reply.OK = true
    px.mu.Lock()
    px.instances[args.Seq] = ins
    px.mu.Unlock()
  } else {
    reply.OK = false
  }

  return nil
}

func (px * Paxos) Decided(seq int, v interface{}) {
  px.mu.Lock()
  ins, exist := px.instances[seq]
  px.mu.Unlock()

  if !exist {
    ins = Instance{}
    ins.Seq = seq
    ins.N_P = -1
    ins.N_A = -1
  }
  ins.Val = v
  px.mu.Lock()
  px.instances[seq] = ins
  px.mu.Unlock()

  args := &DecidedArgs{}
  args.Seq = ins.Seq
  args.Val = v
  args.Me = px.me
  px.mu.Lock()
  args.Doneval = px.donevals[px.me]
  px.mu.Unlock()
  reply := &DecidedReply{}



  for i := range px.peers {
    if i != px.me {
      ok := false
      for j := 0; j < 10 && !ok; j++ {
        ok = call(px.peers[i], "Paxos.DecidedHandler", args, reply)
        if ok {
          //px.Done(reply.Doneval)
          px.mu.Lock()
          px.donevals[i] = reply.Doneval
          px.mu.Unlock()

        }
      }

    }

  }

  px.Done(args.Doneval)

  //px.Log()

}

func (px *Paxos) DecidedHandler(args *DecidedArgs, reply *DecidedReply) error {
  //px.Done(args.Doneval)

  px.mu.Lock()
  px.donevals[args.Me] = args.Doneval
  reply.Doneval = px.donevals[px.me]
  px.mu.Unlock()

  px.Done(reply.Doneval)

  px.mu.Lock()
  ins := px.instances[args.Seq]
  if ins.Val == nil {
	  ins.Val = args.Val
	  px.instances[args.Seq] = ins
	  //px.Log()
  }

  px.mu.Unlock()
  reply.OK = true



  return nil
}

func (px *Paxos) Follow(seq int) {
  //defer log.Println("me:", px.me, "seq:", seq)
  decided, _ := px.Status(seq)
  if decided == false {
    defer log.Println("me:", px.me, "seq:", seq)
    args := &FollowArgs{}
    args.Seq = seq

    for i := range px.peers {
      if i != px.me {
        ok := false
        for j := 0; j < 10 && !ok; j++ {
          reply := &FollowReply{}
          ok = call(px.peers[i], "Paxos.FollowHandler", args, reply)
          if ok {
            //px.Done(reply.Doneval)
            if reply.OK {
              px.mu.Lock()

              ins := px.instances[seq]
              ins.Val = reply.Val
              px.instances[seq] = ins
              px.mu.Unlock()
              return

            } else {
              break
            }
          }
        }
      }

    }
  }

}

func (px *Paxos) FollowHandler(args *FollowArgs, reply *FollowReply) error {
  decided, val := px.Status(args.Seq)
  if decided {
    reply.OK = true
    reply.Val = val
  } else {
    reply.OK = false
  }
  return nil
}

func (px *Paxos) PID() int {
  ti := time.Now()

	//s1 := ti.Format("02012006150405")
  h,m,s := ti.Clock()
  s4 := strconv.Itoa(h)
  s5 := strconv.Itoa(m)
  s6 := strconv.Itoa(s)
  s2 := strconv.Itoa(ti.Nanosecond()/100000)
  s3 := strconv.Itoa(px.me)

  var b bytes.Buffer
  b.WriteString(s4)
  b.WriteString(s5)
  b.WriteString(s6)
  b.WriteString(s2)
  b.WriteString(s3)
  res, _ := strconv.Atoi(b.String())
  return res
}

//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
  if seq < px.Min() {
    return
  } else {
    runtime.GC()
    //fmt.Println(px.me,"server starts seq =", seq, "v =",v)
    go px.Propose(seq, v)
  }
}

func (px *Paxos) Propose(seq int, v interface{}) {
  // Your code here.
  decided, _ := px.Status(seq)
  for !decided && !px.dead {
    pid := px.PID()

    pok, val := px.Prepare(seq, pid)
    // fmt.Println(px.me,"server Pre seq:",seq,"pid:", pid,"pok:",pok,"v:",v,"val:",val)
    // fmt.Println(px.instances)

    if pok {
      //fmt.Println(px.me,"server Pre seq:",seq,"pid:", pid,"pok:",pok,"v:",v,"val:",val)
      if val == nil {
        val = v
      }

      aok := px.Accept(seq, pid, val)
      // fmt.Println(px.me,"server Acc seq:",seq,"pid:", pid,"aok:",aok,"v:",v,"val:",val)
      // fmt.Println(px.instances)


      if aok {
        //fmt.Println(px.me,"server Acc seq:",seq,"pid:", pid,"aok:",aok,"v:",v,"val:",val)
        px.Decided(seq, val)
        //fmt.Println(px.me,"server Acc seq:",seq,"pid:", pid,"aok:",aok,"v:",v,"val:",val)
      }
    }

    decided,_ = px.Status(seq)
    if !decided {
      time.Sleep(5 * PingInterval)
    }

  }


}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
  // Your code here.

  px.mu.Lock()
  dv := px.donevals[px.me]
  px.mu.Unlock()

  for dv + 1 <= seq {
    decided, _ := px.Status(dv + 1)
    if decided {
      dv++
    } else {
      break
    }
  }



  if dv <= seq {
    min := px.Min() - 1
    px.mu.Lock()
    px.donevals[px.me] = dv
    px.mu.Unlock()
    if dv < min {
      min = dv
    }

    px.mu.Lock()
    for i := range px.instances {
      if i <= min {
        delete(px.instances, i)
      }
    }
    px.mu.Unlock()
  }
}

//return the biggest seq of decided instance
func (px *Paxos) Dmax() int {
  px.mu.Lock()
  defer px.mu.Unlock()

  max := px.donevals[px.me]
  for i := range px.instances {
    if i > max && px.instances[i].Val != nil {
      max = i
    }
  }
  return max

}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
  // Your code here.
  px.mu.Lock()
  defer px.mu.Unlock()
  max := -1
  for i := range px.instances {
    if i > max {
      max = i
    }
  }
  return max
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// It is illegal to call Done(i) on a peer and
// then call Start(j) on that peer for any j <= i.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
//
func (px *Paxos) Min() int {
  // You code here.
  px.mu.Lock()
  defer px.mu.Unlock()
  min := px.donevals[px.me]
  for i := range px.peers {
    if px.donevals[i] < min {
      min = px.donevals[i]
    }
  }

  return min + 1
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (bool, interface{}) {
  // Your code here.
  min := px.Min()
  px.mu.Lock()
  defer px.mu.Unlock()
  if seq < min {
    return true, nil
  }
  ins, exist := px.instances[seq]
  if exist && ins.Val != nil {
    return true, ins.Val
  }
  return false, nil
}



//
// tell the peer to shut itself down.
// for testing.
// please do not change this function.
//
func (px *Paxos) Kill() {
  px.dead = true
  if px.l != nil {
    px.l.Close()
  }
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
  px := &Paxos{}
  px.peers = peers
  px.me = me


  // Your initialization code here.
  px.instances = map[int]Instance{}
  px.donevals = map[int]int{}
  for i := range peers {
    px.donevals[i] = -1
  }

  //log.SetPrefix("[" + strconv.Itoa(me) + "]")

  //errFile, _:=os.OpenFile("errors.log",os.O_CREATE|os.O_WRONLY, 0666);



  if rpcs != nil {
    // caller will create socket &c
    rpcs.Register(px)
  } else {
    rpcs = rpc.NewServer()
    rpcs.Register(px)

    // prepare to receive connections from clients.
    // change "unix" to "tcp" to use over a network.
    os.Remove(peers[me]) // only needed for "unix"
    l, e := net.Listen("unix", peers[me]);
    if e != nil {
      log.Fatal("listen error: ", e);
    }
    px.l = l

    // please do not change any of the following code,
    // or do anything to subvert it.

    // create a thread to accept RPC connections
    go func() {
      for px.dead == false {
        conn, err := px.l.Accept()
        if err == nil && px.dead == false {
          if px.unreliable && (rand.Int63() % 1000) < 100 {
            // discard the request.
            conn.Close()
          } else if px.unreliable && (rand.Int63() % 1000) < 200 {
            // process the request but force discard of reply.
            c1 := conn.(*net.UnixConn)
            f, _ := c1.File()
            err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
            if err != nil {
              fmt.Printf("shutdown: %v\n", err)
            }
            px.rpcCount++
            go rpcs.ServeConn(conn)
          } else {
            px.rpcCount++
            go rpcs.ServeConn(conn)
          }
        } else if err == nil {
          conn.Close()
        }
        if err != nil && px.dead == false {
          fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
        }
      }
    }()
  }


  return px
}

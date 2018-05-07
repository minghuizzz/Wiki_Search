package paxos
import "time"
const PingInterval = time.Millisecond * 100

type Instance struct {
  Seq int
  Val interface{} // decided value
  N_P int
  N_A int
  V_A interface{}
}

type PrepareArgs struct {
  Seq int
  PID int
  Me int
  Doneval int
}

type PrepareReply struct {
  OK bool
  N_A int
  V_A interface{}
  Doneval int
  N_P int
}

type AcceptArgs struct {
  Seq int
  PID int
  Val interface{}
  Me int
  Doneval int
}

type AcceptReply struct {
  OK bool
  Doneval int
  V_D interface{}
}

type DecidedArgs struct {
  Seq int
  PID int
  Val interface{}
  Me int
  Doneval int
}

type DecidedReply struct {
  OK bool
  Doneval int
}

type FollowArgs struct {
  Seq int
}

type FollowReply struct {
  OK bool
  Val interface{}
}

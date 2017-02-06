


# 使用GO 语言实现一个分布式KV数据库


#part1

主要是依赖之前的 分布式raft 一直性协议,这里即是写一个client 和server 来存储 或读取数据,代码又有注释这里就不在赘述了


主要提供 Get, Put   Append 操作


Get 操作代码

主要是通过,key 来获取相应的value, 如果key 不存在,那么会是空""
'''

    
func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	entry := Op{Kind:"Get",Key:args.Key,Id:args.Id,ReqId:args.ReqID}

	ok := kv.AppendEntryToLog(entry)
	if !ok {
		reply.WrongLeader = true
	} else {
		reply.WrongLeader = false

		reply.Err = OK
		kv.mu.Lock()
		reply.Value = kv.db[args.Key]
		kv.ack[args.Id] = args.ReqID
		//log.Printf("%d get:%v value:%s\n",kv.me,entry,reply.Value)
		kv.mu.Unlock()
	}
}
'''

方法

func (kv *RaftKV) AppendEntryToLog(entry Op) bool
主要调用之前 raft 那边的Start 方法,将日志记录到raft 中的log[]之中,之所以这样写是更好地方便判断该KEY 是否存在



方法Put 操作
不言而喻,将相应的key value 放入到log[] 中,当然这里为了方便用于更好地实现,这里将数据里面的值 存在 map 中 在apply 方法中是实现
'''

    func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
    	// Your code here.
    	entry := Op{Kind:args.Op,Key:args.Key,Value:args.Value,Id:args.Id,ReqId:args.ReqID}
    	ok := kv.AppendEntryToLog(entry)
    	if !ok {
    		reply.WrongLeader = true
    	} else {
    		reply.WrongLeader = false
    		reply.Err = OK
    	}
    }
    
'''



'''
func (kv *RaftKV) Apply(args Op) {
	switch args.Kind {
	case "Put":
		kv.db[args.Key] = args.Value
	case "Append":
		kv.db[args.Key] += args.Value
	}
	kv.ack[args.Id] = args.ReqId
}

'''


由于raft sever 需要等待 raft 达成一致,在开始的时候操作 put  get 的时候 需要时用 applyCh chan,避免找出死锁 


raft server 在启动之后需要立即返回,所以在真正实现的逻辑的时候使用go routing,通过不同的类型,不同的处理

'''
    
    go func() {
    		for {
    			msg := <-kv.applyCh
    			if msg.UseSnapshot {
    				var LastIncludedIndex int
    				var LastIncludedTerm int
    
    				r := bytes.NewBuffer(msg.Snapshot)
    				d := gob.NewDecoder(r)
    
    				kv.mu.Lock()
    				d.Decode(&LastIncludedIndex)
    				d.Decode(&LastIncludedTerm)
    				kv.db = make(map[string]string)
    				kv.ack = make(map[int64]int)
    				d.Decode(&kv.db)
    				d.Decode(&kv.ack)
    				kv.mu.Unlock()
    			} else {
    				op := msg.Command.(Op)
    				kv.mu.Lock()
    				if !kv.CheckDup(op.Id,op.ReqId) {
    					kv.Apply(op)
    				}
    
    				ch,ok := kv.result[msg.Index]
    				if ok {
    					select {
    					case <-kv.result[msg.Index]:
    					default:
    					}
    					ch <- op
    				} else {
    					kv.result[msg.Index] = make(chan Op, 1)
    				}
    
    				//need snapshot
    				if maxraftstate != -1 && kv.rf.GetPerisistSize() > maxraftstate {
    					w := new(bytes.Buffer)
    					e := gob.NewEncoder(w)
    					e.Encode(kv.db)
    					e.Encode(kv.ack)
    					data := w.Bytes()
    					go kv.rf.StartSnapshot(data,msg.Index)
    				}
    				kv.mu.Unlock()
    			}
    		}
    	}()

    
'''







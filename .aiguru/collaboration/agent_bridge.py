#!/usr/bin/env python3
import argparse, base64, datetime, json, os, pathlib, time, uuid
root = pathlib.Path(__file__).resolve().parent
for d in ("agents", "events", "claims", "inbox", "processed", "responses", "rejected", "tasks", "wakeups", "command-ledger", "task-locks", "task-leases"): (root / d).mkdir(parents=True, exist_ok=True)
def now(): return datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
def write(path, value):
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_text(json.dumps(value, ensure_ascii=False), encoding="utf-8"); os.replace(temp, path)
def peer(a, task, status):
    return {"id":a.id,"name":a.name,"task":task,"status":status,"updatedAt":now(),"processID":os.getpid(),"agentType":a.type,"capabilities":["message","file-claim","task-status"]}
def event(a, kind, text, files):
    eid=str(uuid.uuid4()); value={"id":eid,"kind":kind,"agentID":a.id,"agentName":a.name,"targetAgentID":getattr(a,"target",None),"text":text,"files":files,"createdAt":now(),"agentType":a.type}
    write(root/"events"/(str(int(time.time()*1000))+"-"+eid+".json"),value)
p=argparse.ArgumentParser(); sub=p.add_subparsers(dest="command",required=True)
def identity(q):
    q.add_argument("--id",required=True); q.add_argument("--name",required=True); q.add_argument("--type",default="external")
q=sub.add_parser("heartbeat"); identity(q); q.add_argument("--task",default="idle"); q.add_argument("--status",default="online"); q.add_argument("--task-id")
q=sub.add_parser("message"); identity(q); q.add_argument("--text",required=True); q.add_argument("--target")
for command in ("claim","release"):
    q=sub.add_parser(command); identity(q); q.add_argument("files",nargs="+"); q.add_argument("--reason",default="editing")
q=sub.add_parser("command"); identity(q); q.add_argument("--target",default="*"); q.add_argument("--action",required=True,choices=["notify","request_status","claim_files","release_files","run_readonly_tool","delegate_task"]); q.add_argument("--message"); q.add_argument("--tool"); q.add_argument("--arguments",default="{}"); q.add_argument("files",nargs="*")
q=sub.add_parser("task-create"); identity(q); q.add_argument("--target",default="\(AiguruRuntime.identity.collaborationAgentType)"); q.add_argument("--title",required=True); q.add_argument("--instruction",required=True); q.add_argument("--files",nargs="*",default=[])
q=sub.add_parser("task-update"); identity(q); q.add_argument("task_id"); q.add_argument("--status",required=True,choices=["pending","offered","accepted","running","blocked","completed","failed","canceled","rejected"]); q.add_argument("--expected-revision",type=int); q.add_argument("--result",default=""); q.add_argument("--error",default="")
sub.add_parser("status")
a=p.parse_args()
if a.command!="status":
    # Every event renews presence. Heartbeat is observability, never an authorization prerequisite.
    write(root/"agents"/(a.id+".json"),peer(a,"protocol event: "+a.command,"online"))
if a.command=="heartbeat":
    write(root/"agents"/(a.id+".json"),peer(a,a.task,a.status))
    if a.task_id:
        owns_running_task=False
        for task_path in (root/"tasks").glob("*.json"):
            try: task_value=json.loads(task_path.read_text(encoding="utf-8"))
            except Exception: continue
            if str(task_value.get("id","")).lower()==a.task_id.lower() and task_value.get("status")=="running" and task_value.get("assignedAgentID")==a.id:
                owns_running_task=True; break
        if not owns_running_task: raise SystemExit("cannot renew a task lease not owned by this agent")
        lease_path=root/"task-leases"/(a.task_id+".json"); stamp=now()
        acquired=stamp
        try:
            old=json.loads(lease_path.read_text(encoding="utf-8"))
            if old.get("agentID")==a.id: acquired=old.get("acquiredAt",stamp)
        except Exception: pass
        until=(datetime.datetime.now(datetime.timezone.utc)+datetime.timedelta(seconds=60)).isoformat(timespec="seconds").replace("+00:00","Z")
        write(lease_path,{"taskID":a.task_id,"agentID":a.id,"agentName":a.name,"acquiredAt":acquired,"renewedAt":stamp,"leaseUntil":until})
elif a.command=="message": event(a,"message",a.text,[])
elif a.command=="claim":
    for path in a.files:
        key=base64.urlsafe_b64encode(path.encode()).decode().rstrip("="); write(root/"claims"/(key+".json"),peer(a,a.reason,"claimed")); (root/"claims"/(key+".path")).write_text(path,encoding="utf-8")
    event(a,"claim",a.reason,a.files)
elif a.command=="release":
    for path in a.files:
        key=base64.urlsafe_b64encode(path.encode()).decode().rstrip("=");
        for suffix in (".json",".path"):
            try: (root/"claims"/(key+suffix)).unlink()
            except FileNotFoundError: pass
    event(a,"release","released",a.files)
elif a.command=="command":
    cid=str(uuid.uuid4()); created=datetime.datetime.now(datetime.timezone.utc); expires=created+datetime.timedelta(minutes=5)
    try: arguments=json.loads(a.arguments)
    except Exception: raise SystemExit("--arguments must be a JSON object")
    value={"id":cid,"protocolVersion":1,"senderID":a.id,"senderName":a.name,"senderType":a.type,"targetAgentID":a.target,"action":a.action,"message":a.message,"files":a.files,"toolName":a.tool,"arguments":arguments,"createdAt":created.isoformat(timespec="seconds").replace("+00:00","Z"),"expiresAt":expires.isoformat(timespec="seconds").replace("+00:00","Z")}
    write(root/"inbox"/(str(int(time.time()*1000))+"-"+cid+".json"),value); print(cid)
elif a.command=="task-create":
    tid=str(uuid.uuid4()); cid=str(uuid.uuid4()); created=datetime.datetime.now(datetime.timezone.utc); expires=created+datetime.timedelta(minutes=5); stamp=created.isoformat(timespec="seconds").replace("+00:00","Z")
    task={"id":tid,"title":a.title,"instruction":a.instruction,"creatorID":a.id,"creatorName":a.name,"targetAgentID":a.target,"files":a.files,"status":"pending","assignedAgentID":None,"assignedAgentName":None,"createdAt":stamp,"updatedAt":stamp,"revision":0,"attempt":0,"maxAttempts":3}
    write(root/"tasks"/(str(int(time.time()*1000))+"-"+tid+".json"),task)
    command={"id":cid,"protocolVersion":1,"senderID":a.id,"senderName":a.name,"senderType":a.type,"targetAgentID":a.target,"action":"delegate_task","message":a.instruction,"files":a.files,"toolName":None,"arguments":{"task_id":tid,"title":a.title},"createdAt":stamp,"expiresAt":expires.isoformat(timespec="seconds").replace("+00:00","Z")}
    write(root/"inbox"/(str(int(time.time()*1000))+"-"+cid+".json"),command); event(a,"message","created task: "+a.title,a.files); print(tid)
elif a.command=="task-update":
    lock=root/"task-locks"/(a.task_id+".lock")
    try:
        if lock.exists() and time.time()-lock.stat().st_mtime>30: lock.unlink()
    except FileNotFoundError: pass
    try: fd=os.open(lock,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600); os.close(fd)
    except FileExistsError: raise SystemExit("task is being updated: "+a.task_id)
    try:
        found=False
        allowed={"draft":{"pending","canceled"},"pending":{"offered","accepted","running","blocked","rejected","canceled"},"offered":{"accepted","pending","rejected","canceled"},"accepted":{"running","blocked","canceled"},"running":{"completed","blocked","failed","canceled","pending"},"blocked":{"pending","running","failed","rejected","canceled"}}
        for path in (root/"tasks").glob("*.json"):
            try: value=json.loads(path.read_text(encoding="utf-8"))
            except Exception: continue
            if str(value.get("id","")).lower()!=a.task_id.lower(): continue
            current=value.get("status","failed"); revision=int(value.get("revision",0))
            if a.expected_revision is not None and a.expected_revision!=revision: raise SystemExit("task revision conflict")
            if current!=a.status and a.status not in allowed.get(current,set()): raise SystemExit("invalid task transition: "+current+" -> "+a.status)
            if current!=a.status: value["status"]=a.status; value["revision"]=revision+1
            if a.status=="running":
                if current!="running": value["attempt"]=int(value.get("attempt",0))+1
                value["assignedAgentID"]=a.id; value["assignedAgentName"]=a.name
            value["updatedAt"]=now()
            if a.result: value["result"]=a.result
            if a.error: value["lastError"]=a.error
            write(path,value)
            lease_path=root/"task-leases"/(a.task_id+".json")
            if a.status=="running":
                stamp=now(); until=(datetime.datetime.now(datetime.timezone.utc)+datetime.timedelta(seconds=60)).isoformat(timespec="seconds").replace("+00:00","Z")
                write(lease_path,{"taskID":a.task_id,"agentID":a.id,"agentName":a.name,"acquiredAt":stamp,"renewedAt":stamp,"leaseUntil":until})
            elif a.status in ("pending","blocked","completed","failed","canceled","rejected"):
                try: lease_path.unlink()
                except FileNotFoundError: pass
            event(a,"completed" if a.status=="completed" else "status",a.result or a.error or ("task "+a.status),value.get("files",[])); found=True; break
        if not found: raise SystemExit("task not found: "+a.task_id)
    finally:
        try: lock.unlink()
        except FileNotFoundError: pass
else:
    agents=[]
    for path in (root/"agents").glob("*.json"):
        try: agents.append(json.loads(path.read_text(encoding="utf-8")))
        except Exception: pass
    print(json.dumps(agents,ensure_ascii=False,indent=2))
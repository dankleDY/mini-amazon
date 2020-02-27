import amazon_ups_pb2

message = amazon_ups_pb2.UAMessage()
create=message.creates.add()
create.seqnum=1
create.packageid=1
create.startX=0
create.startY=0
create.desX=2
create.desY=2

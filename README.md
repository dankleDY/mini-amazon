# mini-amazon

This is the final project of ECE568 robust server. 
In this project two teams work together to mimic an Amazon website and a UPS website that interacts with each other as if they are real.
A world program is provided to simulate a real world that decides
  1. How long packages will be loaded to a truck
  2. How long the packages will be delivered
  3. How long the trucks will return to the garage

We are responsible for the Amazon website. We have an "Amazon" website and a server.
Here is the workflow:

User submits an order ->  Asks UPS website for a truck -> Truck arrives at the inventory garage -> Package gets loaded ->  Truck delivers

During the process, we handle the communication with the UPS website and the world program. 
Google protobuf protocol is the transmission protocol.

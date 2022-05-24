import random
import sys

nodes = int(sys.argv[1])
times = int(sys.argv[2])
message = 'Hello!'


def percentage(part, whole):
    return 100 * float(part)/float(whole)


class GossipNode:
    available_nodes = []
    infected_nodes = []

    def __init__(self, number, message=None):
        DEFAULT_MESSAGE = 1
        self.number = number
        self.message = message if message is not None else DEFAULT_MESSAGE

    def send_message(self, message):
        selected_nodes = random.choices(self.available_nodes, k=5)
        for node in selected_nodes:
            node.recieve_message(message)

    def recieve_message(self, message):
        while self.message != message:
            self.message = message
            GossipNode.infected_nodes.append(self.number)
            self.send_message(message)


def main(nodes, message):
    sucsess = 0
    for i in range(times):
        GossipNode.infected_nodes = []
        GossipNode.available_nodes = []
        for node in range(nodes):
            node = GossipNode(node)
            GossipNode.available_nodes.append(node)
        GossipNode.available_nodes[0].recieve_message(message)
        if len(GossipNode.infected_nodes) == len(GossipNode.available_nodes):
            sucsess += 1

    print('In {}% cases all nodes received the message'.format(percentage(sucsess, times)))


if __name__ == '__main__':
    main(nodes, message)

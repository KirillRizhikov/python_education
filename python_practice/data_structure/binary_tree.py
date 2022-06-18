"""
Binary Search Tree
        insert - добавить элемент,
        lookup - найти элемент по значению и вернуть ссылку на него (узел),
        delete - удалить элемент по значению),
"""
from random import randrange
import logging

logger = logging.getLogger("Tree")
logger.setLevel(logging.ERROR)
# formatter = logging.Formatter('%(asctime)s:%(name)s:%(message)s')
stream_handler = logging.StreamHandler()
logger.addHandler(stream_handler)


class BinaryTreeNode:
    def __init__(self, data):
        self.data = data
        self.left = None
        self.right = None
        self.parent = None


class BinaryTree:

    def __init__(self):
        self.root = None

    def isert(self, data):
        node = BinaryTreeNode(data)
        if self.root is None:
            self.root = node
            return id(self.root)
        current_node = self.root
        while True:
            if current_node.data == node.data:
                break
            if current_node.data > node.data and current_node.left is None:
                current_node.left = node
                node.parent = current_node
                logger.debug(f"insert left {current_node.data}")
                break
            if current_node.data < node.data and current_node.right is None:
                current_node.right = node
                node.parent = current_node
                logger.debug(f"insert right {current_node.data}")
                break
            if current_node.data > node.data:
                current_node = current_node.left
            else:
                current_node = current_node.right
        return id(node)

    def lookup(self, data):
        if self.root is None:
            return 'Tree is empty'
        current_node = self.root
        while True:
            if current_node.data == data:
                return current_node
            elif current_node.data > data and current_node.left != None:
                current_node = current_node.left
            elif current_node.data < data and current_node.right != None:
                current_node = current_node.right
            else:
                return False

    def find_max(self, root):
        current_node = root
        while current_node.right:
            current_node = current_node.right
        return current_node

    def find_min(self, root):
        current_node = root
        while current_node.left:
            current_node = current_node.left
        return current_node

    def del_link(self, node):
        if node.parent.right == node:
            node.parent.right = None
        else:
            node.parent.left = None

    def delete(self, data):
        current_node = self.lookup(data)
        if current_node == False:
            return "no such value"

        elif current_node.data == self.root.data:
            if current_node.left is None and current_node.right is None:
                self.root = None

        elif current_node.left is None and current_node.right is None:
            self.del_link(current_node)

        elif current_node.left is None:
            sub_node = self.find_min(current_node.right)
            current_node.data = sub_node.data
            self.del_link(sub_node)

        else:
            sub_node = self.find_max(current_node.left)
            current_node.data = sub_node.data
            self.del_link(sub_node)


tree = BinaryTree()

for i in range(10):
    rand = randrange(10)
    tree.isert(rand)
    logger.debug(f"insert {rand}")

print(tree.lookup(9))

tree.delete(9)
print(tree.lookup(9))

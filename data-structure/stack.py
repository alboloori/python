import collections
#=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
class TreeNode:
    def __init__(self,data=None):
        self.data=data
        self.left=None
        self.right=None
#=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
class Node:
    def __init__(self,data):
        self.data=data
        self.next=None
#=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
class Stack:
    def __init__(self):
        self.top=None
        self.size=0
    def push(self,data):
        node = Node(data)
        if self.top:
            node.next=self.top
            self.top=node
        else:
            self.top=node
        size+=1
    def pop(self):
        if self.top:
            data=self.top.data
            size-=1
            if self.top.next:
                self.top=self.top.next
            else: self.top=None
            return data
        else: 
            return None 
    
#=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
def calc(node):
        if node.data is "+":
            return calc(node.left) + calc(node.right)
        elif node.data is "-":
            return calc(node.left) - calc(node.right)
        elif node.data is "*":
            return calc(node.left) * calc(node.right)
        elif node.data is "/":
            return calc(node.left) / calc(node.right)
        else:
            return node.data
#=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
stack =Stack()
expr=str(input()) # type: ignore
for term in expr:
    if term in '*/+-':
        node=TreeNode(term)
        node.left=stack.pop()
        node.right=stack.pop()
    elif term not in('({[]})')
        node=TreeNode(int(term))
        stack.push(node)
root = stack.pop()
result = calc(root)
print(result)

    
class BTreeNode:
    def __init__(self, leaf=True):
        self.leaf = leaf
        self.keys = []
        self.children = []
        self.values = []  # Store actual data values

class BTree:
    def __init__(self, t=3):  # t is the minimum degree
        self.root = BTreeNode(True)
        self.t = t

    def search(self, key):
        return self._search(self.root, key)

    def _search(self, node, key):
        i = 0
        while i < len(node.keys) and key > node.keys[i]:
            i += 1
        
        if i < len(node.keys) and key == node.keys[i]:
            return node.values[i]
        
        if node.leaf:
            return None
        
        return self._search(node.children[i], key)

    def update(self, key, new_value):
        """Update value for existing key"""
        return self._update(self.root, key, new_value)

    def _update(self, node, key, new_value):
        i = 0
        while i < len(node.keys) and key > node.keys[i]:
            i += 1
        
        if i < len(node.keys) and key == node.keys[i]:
            node.values[i] = new_value
            return True
        
        if node.leaf:
            return False
        
        return self._update(node.children[i], key, new_value)

    def insert(self, key, value):
        root = self.root
        if len(root.keys) == (2 * self.t) - 1:
            new_root = BTreeNode(False)
            new_root.children.append(root)
            self._split_child(new_root, 0)
            self.root = new_root
            self._insert_non_full(new_root, key, value)
        else:
            self._insert_non_full(root, key, value)

    def _insert_non_full(self, node, key, value):
        i = len(node.keys) - 1
        
        if node.leaf:
            while i >= 0 and key < node.keys[i]:
                i -= 1
            node.keys.insert(i + 1, key)
            node.values.insert(i + 1, value)
        else:
            while i >= 0 and key < node.keys[i]:
                i -= 1
            i += 1
            
            if len(node.children[i].keys) == (2 * self.t) - 1:
                self._split_child(node, i)
                if key > node.keys[i]:
                    i += 1
            
            self._insert_non_full(node.children[i], key, value)

    def _split_child(self, parent, index):
        t = self.t
        child = parent.children[index]
        new_node = BTreeNode(child.leaf)
        
        parent.children.insert(index + 1, new_node)
        parent.keys.insert(index, child.keys[t - 1])
        parent.values.insert(index, child.values[t - 1])
        
        new_node.keys = child.keys[t:]
        new_node.values = child.values[t:]
        child.keys = child.keys[:t - 1]
        child.values = child.values[:t - 1]
        
        if not child.leaf:
            new_node.children = child.children[t:]
            child.children = child.children[:t]

    def delete(self, key):
        self._delete(self.root, key)
        if len(self.root.keys) == 0 and not self.root.leaf:
            self.root = self.root.children[0]

    def _delete(self, node, key):
        t = self.t
        i = 0
        
        while i < len(node.keys) and key > node.keys[i]:
            i += 1
            
        if i < len(node.keys) and key == node.keys[i]:
            if node.leaf:
                node.keys.pop(i)
                node.values.pop(i)
            else:
                self._delete_from_non_leaf(node, i)
        else:
            if node.leaf:
                return
            
            if len(node.children[i].keys) < t:
                self._fill(node, i)
                
            if i > len(node.keys):
                i -= 1
                
            self._delete(node.children[i], key)

    def _delete_from_non_leaf(self, node, index):
        key = node.keys[index]
        
        if len(node.children[index].keys) >= self.t:
            predecessor = self._get_predecessor(node, index)
            node.keys[index] = predecessor[0]
            node.values[index] = predecessor[1]
            self._delete(node.children[index], predecessor[0])
        elif len(node.children[index + 1].keys) >= self.t:
            successor = self._get_successor(node, index)
            node.keys[index] = successor[0]
            node.values[index] = successor[1]
            self._delete(node.children[index + 1], successor[0])
        else:
            self._merge(node, index)
            self._delete(node.children[index], key)

    def _get_predecessor(self, node, index):
        current = node.children[index]
        while not current.leaf:
            current = current.children[-1]
        return current.keys[-1], current.values[-1]

    def _get_successor(self, node, index):
        current = node.children[index + 1]
        while not current.leaf:
            current = current.children[0]
        return current.keys[0], current.values[0]

    def _fill(self, node, index):
        if index != 0 and len(node.children[index - 1].keys) >= self.t:
            self._borrow_from_prev(node, index)
        elif index != len(node.keys) and len(node.children[index + 1].keys) >= self.t:
            self._borrow_from_next(node, index)
        else:
            if index != len(node.keys):
                self._merge(node, index)
            else:
                self._merge(node, index - 1)

    def _borrow_from_prev(self, node, index):
        child = node.children[index]
        sibling = node.children[index - 1]
        
        child.keys.insert(0, node.keys[index - 1])
        child.values.insert(0, node.values[index - 1])
        
        if not child.leaf:
            child.children.insert(0, sibling.children.pop())
            
        node.keys[index - 1] = sibling.keys.pop()
        node.values[index - 1] = sibling.values.pop()

    def _borrow_from_next(self, node, index):
        child = node.children[index]
        sibling = node.children[index + 1]
        
        child.keys.append(node.keys[index])
        child.values.append(node.values[index])
        
        if not child.leaf:
            child.children.append(sibling.children.pop(0))
            
        node.keys[index] = sibling.keys.pop(0)
        node.values[index] = sibling.values.pop(0)

    def _merge(self, node, index):
        child = node.children[index]
        sibling = node.children[index + 1]
        
        child.keys.append(node.keys[index])
        child.values.append(node.values[index])
        
        child.keys.extend(sibling.keys)
        child.values.extend(sibling.values)
        
        if not child.leaf:
            child.children.extend(sibling.children)
            
        node.keys.pop(index)
        node.values.pop(index)
        node.children.pop(index + 1) 
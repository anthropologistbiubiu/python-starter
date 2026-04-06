

class Node:
    def __init__(self):
        self.children = {}
        self.is_End_word = False

class Trie:
    def __init__(self):
        self.root = Node()
    def Insert(self,word):
        node = self.root
        for  char in word:
            if char in node.children:
               node = node.children[char]
            else:
                node.children[char] = Node()
                node = node.children[char]
        node.is_End_word = True


    def search(self,word):
        node = self.root
        for char in word:
            if char not in node.children:
               return False
            else:
                node = node.children[char]
        return node.is_End_word

    def starts_with(self,prefix):
        node = self.root
        for char in prefix:
            if char not in node.children:
                return False
            else:
                node = node.children[char]
        return node.is_End_word





def main():
    trie = Trie()
    trie.Insert("apple")

if __name__ == '__main__':
    main()







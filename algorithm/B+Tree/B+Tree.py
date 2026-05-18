from __future__ import annotations

from dataclasses import dataclass, field
from bisect import bisect_left, bisect_right
from typing import Optional, Any
import random


@dataclass
class Node:
    page_no: int
    is_leaf: bool
    keys: list[int] = field(default_factory=list)

    # 内部节点使用 children
    children: list["Node"] = field(default_factory=list)

    # 叶子节点使用 values
    values: list[Any] = field(default_factory=list)

    parent: Optional["Node"] = None

    # 叶子节点链表
    next: Optional["Node"] = None

    def label(self) -> str:
        node_type = "Leaf" if self.is_leaf else "Internal"
        return f"{node_type}#P{self.page_no}{self.keys}"


class BPlusTree:
    def __init__(self, max_keys: int = 3):
        self.max_keys = max_keys
        self.next_page_no = 1
        self.root = self._new_node(is_leaf=True)

    def _new_node(self, is_leaf: bool) -> Node:
        node = Node(page_no=self.next_page_no, is_leaf=is_leaf)
        self.next_page_no += 1
        return node

    def insert(self, key: int, value: Any = None):
        leaf = self._find_leaf(key)
        pos = bisect_left(leaf.keys, key)

        # key 已存在，直接更新
        if pos < len(leaf.keys) and leaf.keys[pos] == key:
            leaf.values[pos] = value
            return

        leaf.keys.insert(pos, key)
        leaf.values.insert(pos, value)

        if len(leaf.keys) > self.max_keys:
            self._split_leaf(leaf)

    def search(self, key: int) -> Optional[Any]:
        leaf = self._find_leaf(key)
        pos = bisect_left(leaf.keys, key)

        if pos < len(leaf.keys) and leaf.keys[pos] == key:
            return leaf.values[pos]

        return None

    def _find_leaf(self, key: int) -> Node:
        node = self.root

        while not node.is_leaf:
            index = bisect_right(node.keys, key)
            node = node.children[index]

        return node

    def _split_leaf(self, leaf: Node):
        """
        叶子节点分裂：

        原叶子：
            [10, 20, 30, 40]

        分裂后：
            left  = [10, 20]
            right = [30, 40]

        上推到父节点的 key：
            right.keys[0]
        """
        mid = (len(leaf.keys) + 1) // 2

        right = self._new_node(is_leaf=True)

        right.keys = leaf.keys[mid:]
        right.values = leaf.values[mid:]

        leaf.keys = leaf.keys[:mid]
        leaf.values = leaf.values[:mid]

        # 维护叶子链表
        right.next = leaf.next
        leaf.next = right

        right.parent = leaf.parent

        promote_key = right.keys[0]

        self._insert_into_parent(left=leaf, key=promote_key, right=right)

    def _insert_into_parent(self, left: Node, key: int, right: Node):
        parent = left.parent

        # left 是 root，没有父节点，说明树要长高
        if parent is None:
            new_root = self._new_node(is_leaf=False)

            new_root.keys = [key]
            new_root.children = [left, right]

            left.parent = new_root
            right.parent = new_root

            self.root = new_root
            return

        # 找到 left 在父节点 children 中的位置
        left_index = parent.children.index(left)

        # 在 left 右边插入 right
        parent.keys.insert(left_index, key)
        parent.children.insert(left_index + 1, right)

        right.parent = parent

        # 父节点也满了，继续分裂内部节点
        if len(parent.keys) > self.max_keys:
            self._split_internal(parent)

    def _split_internal(self, node: Node):
        """
        内部节点分裂：

        Internal[10, 20, 30, 40]

        mid key = 30

        left internal:
            [10, 20]

        right internal:
            [40]

        30 上推到父节点。

        注意：
        叶子节点分裂时，上推 key 仍保留在右叶子节点。
        内部节点分裂时，上推 key 不保留在左右内部节点。
        """
        mid = len(node.keys) // 2
        promote_key = node.keys[mid]

        right = self._new_node(is_leaf=False)

        right.keys = node.keys[mid + 1:]
        right.children = node.children[mid + 1:]

        for child in right.children:
            child.parent = right

        node.keys = node.keys[:mid]
        node.children = node.children[:mid + 1]

        right.parent = node.parent

        self._insert_into_parent(left=node, key=promote_key, right=right)

    # =============================
    # 树状打印相关方法
    # =============================

    def render_tree(self) -> str:
        """
        返回树状结构字符串。
        """
        lines = []
        self._render_node(self.root, prefix="", is_last=True,
                          lines=lines, is_root=True)
        return "\n".join(lines)

    def _render_node(
        self,
        node: Node,
        prefix: str,
        is_last: bool,
        lines: list[str],
        is_root: bool = False
    ):
        if is_root:
            lines.append(node.label())
        else:
            connector = "└── " if is_last else "├── "
            lines.append(prefix + connector + node.label())

        if not node.is_leaf:
            if is_root:
                child_prefix = ""
            else:
                child_prefix = prefix + ("    " if is_last else "│   ")

            for i, child in enumerate(node.children):
                child_is_last = i == len(node.children) - 1
                self._render_node(
                    child,
                    prefix=child_prefix,
                    is_last=child_is_last,
                    lines=lines,
                    is_root=False
                )

    def render_leaf_chain(self) -> str:
        """
        返回叶子链表字符串。
        """
        node = self.root

        while not node.is_leaf:
            node = node.children[0]

        chain = []

        while node:
            chain.append(f"P{node.page_no}{node.keys}")
            node = node.next

        return "Leaf chain: " + " -> ".join(chain)

    def print_tree_pretty(self):
        print(self.render_tree())
        print(self.render_leaf_chain())

    def append_snapshot_to_file(self, filename: str, title: str):
        """
        把当前 B+ 树快照追加写入文件。
        """
        with open(filename, "a", encoding="utf-8") as f:
            f.write(title)
            f.write("\n")
            f.write(self.render_tree())
            f.write("\n")
            f.write(self.render_leaf_chain())
            f.write("\n")
            f.write("=" * 80)
            f.write("\n\n")


if __name__ == "__main__":
    random.seed(7)

    # 随机生成 20 个不重复数字
    data = random.sample(range(1, 100), 20)

    output_file = "bplus_tree_output.txt"

    # 先清空旧文件
    with open(output_file, "w", encoding="utf-8") as f:
        f.write("B+ 树插入过程记录\n")
        f.write(f"随机插入顺序: {data}\n")
        f.write("=" * 80)
        f.write("\n\n")

    tree = BPlusTree(max_keys=3)

    print("随机插入顺序:")
    print(data)

    for x in data:
        print(f"\n插入: {x}")
        tree.insert(x, value=f"value-{x}")

        # 控制台树状打印
        tree.print_tree_pretty()

        # 写入文件
        tree.append_snapshot_to_file(
            filename=output_file,
            title=f"插入: {x}"
        )

    print(f"\n树状打印结果已经写入文件: {output_file}")

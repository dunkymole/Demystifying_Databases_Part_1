using System.Diagnostics;

namespace Database.Core.DataStructures.Trees.RedBlack
{
    internal sealed class Node<T>
    {
        public Node(T item, NodeColor color)
        {
            Item = item;
            Color = color;
        }

        public static bool IsNonNullBlack(Node<T>? node) => node != null && node.IsBlack;

        public static bool IsNonNullRed(Node<T>? node) => node != null && node.IsRed;

        public static bool IsNullOrBlack(Node<T>? node) => node == null || node.IsBlack;

        public T Item { get; internal set; }

        public Node<T>? Left { get; set; }

        public Node<T>? Right { get; set; }

        public NodeColor Color { get; set; }

        public bool IsBlack => Color == NodeColor.Black;

        public bool IsRed => Color == NodeColor.Red;

        public bool Is2Node => IsBlack && IsNullOrBlack(Left) && IsNullOrBlack(Right);

        public bool Is4Node => IsNonNullRed(Left) && IsNonNullRed(Right);

        public void ColorBlack() => Color = NodeColor.Black;

        public void ColorRed() => Color = NodeColor.Red;

        /// <summary>
        /// Gets the rotation this node should undergo during a removal.
        /// </summary>
        public TreeRotation GetRotation(Node<T> current, Node<T> sibling)
        {
#if DEBUG
            Debug.Assert(IsNonNullRed(sibling.Left) || IsNonNullRed(sibling.Right));

            Debug.Assert(HasChildren(current, sibling));
#endif

            bool currentIsLeftChild = Left == current;
            return IsNonNullRed(sibling.Left) ?
                currentIsLeftChild ? TreeRotation.RightLeft : TreeRotation.Right :
                (currentIsLeftChild ? TreeRotation.Left : TreeRotation.LeftRight);
        }

        /// <summary>
        /// Gets the sibling of one of this node's children.
        /// </summary>
        public Node<T> GetSibling(Node<T> node)
        {
            Debug.Assert(node != null);
            Debug.Assert(node == Left ^ node == Right);

            return node == Left ? Right! : Left!;
        }

        public void Split4Node()
        {
            Debug.Assert(Left != null);
            Debug.Assert(Right != null);

            ColorRed();
            Left.ColorBlack();
            Right.ColorBlack();
        }

        /// <summary>
        /// Does a rotation on this tree. May change the color of a grandchild from red to black.
        /// </summary>
        public Node<T>? Rotate(TreeRotation rotation)
        {
            Node<T> removeRed;
            switch (rotation)
            {
                case TreeRotation.Right:
                    removeRed = Left!.Left!;
                    Debug.Assert(removeRed.IsRed);
                    removeRed.ColorBlack();
                    return RotateRight();
                case TreeRotation.Left:
                    removeRed = Right!.Right!;
                    Debug.Assert(removeRed.IsRed);
                    removeRed.ColorBlack();
                    return RotateLeft();
                case TreeRotation.RightLeft:
                    Debug.Assert(Right!.Left!.IsRed);
                    return RotateRightLeft();
                case TreeRotation.LeftRight:
                    Debug.Assert(Left!.Right!.IsRed);
                    return RotateLeftRight();
                default:
                    Debug.Fail($"{nameof(rotation)}: {rotation} is not a defined {nameof(TreeRotation)} value.");
                    return null;
            }
        }

        /// <summary>
        /// Does a left rotation on this tree, making this node the new left child of the current right child.
        /// </summary>
        public Node<T> RotateLeft()
        {
            Node<T> child = Right!;
            Right = child.Left;
            child.Left = this;
            return child;
        }

        /// <summary>
        /// Does a left-right rotation on this tree. The left child is rotated left, then this node is rotated right.
        /// </summary>
        public Node<T> RotateLeftRight()
        {
            Node<T> child = Left!;
            Node<T> grandChild = child.Right!;

            Left = grandChild.Right;
            grandChild.Right = this;
            child.Right = grandChild.Left;
            grandChild.Left = child;
            return grandChild;
        }

        /// <summary>
        /// Does a right rotation on this tree, making this node the new right child of the current left child.
        /// </summary>
        public Node<T> RotateRight()
        {
            Node<T> child = Left!;
            Left = child.Right;
            child.Right = this;
            return child;
        }

        /// <summary>
        /// Does a right-left rotation on this tree. The right child is rotated right, then this node is rotated left.
        /// </summary>
        public Node<T> RotateRightLeft()
        {
            Node<T> child = Right!;
            Node<T> grandChild = child.Left!;

            Right = grandChild.Left;
            grandChild.Left = this;
            child.Left = grandChild.Right;
            grandChild.Right = child;
            return grandChild;
        }

        /// <summary>
        /// Combines two 2-nodes into a 4-node.
        /// </summary>
        public void Merge2Nodes()
        {
            Debug.Assert(IsRed);
            Debug.Assert(Left!.Is2Node);
            Debug.Assert(Right!.Is2Node);

            // Combine two 2-nodes into a 4-node.
            ColorBlack();
            Left.ColorRed();
            Right.ColorRed();
        }

        /// <summary>
        /// Replaces a child of this node with a new node.
        /// </summary>
        /// <param name="child">The child to replace.</param>
        /// <param name="newChild">The node to replace <paramref name="child"/> with.</param>
        public void ReplaceChild(Node<T> child, Node<T> newChild)
        {
#if DEBUG
            Debug.Assert(HasChild(child));
#endif

            if (Left == child)
            {
                Left = newChild;
            }
            else
            {
                Right = newChild;
            }
        }

#if DEBUG
        private bool HasChild(Node<T> child) => child == Left || child == Right;

        private bool HasChildren(Node<T> child1, Node<T> child2)
        {
            Debug.Assert(child1 != child2);

            return Left == child1 && Right == child2
                   || Left == child2 && Right == child1;
        }
#endif
    }
}
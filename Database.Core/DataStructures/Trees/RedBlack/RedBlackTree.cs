using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;

namespace Database.Core.DataStructures.Trees.RedBlack
{
    // A binary search tree is a red-black tree if it satisfies the following red-black properties:
    // 1. Every node is either red or black
    // 2. Every leaf (nil node) is black
    // 3. If a node is red, the both its children are black
    // 4. Every simple path from a node to a descendant leaf contains the same number of black nodes
    //
    // The basic idea of a red-black tree is to represent 2-3-4 trees as standard BSTs but to add one extra bit of information
    // per node to encode 3-nodes and 4-nodes.
    // 4-nodes will be represented as:   B
    //                                 R   R
    //
    // 3 -node will be represented as:   B     or     B
    //                                 R   B        B   R
    //
    // For a detailed description of the algorithm, take a look at "Algorithms" by Robert Sedgewick.


    public class Set<T>
    {
        private int _version;
        private readonly IComparer<T> _comparer;
        private readonly ReaderWriterLockSlim _lock = new ReaderWriterLockSlim(LockRecursionPolicy.SupportsRecursion); // eg AddOrReplace calls other public (therefore lock protected) methods
        private Node<T>? _root;

        public Set(IComparer<T>? comparer = null) 
            => _comparer = comparer ?? Comparer<T>.Default;

        public int Count { get; private set; }
        public int Version => _version;

        public IEnumerable<T> GetItems(bool reverse) => Traverse(reverse: reverse).Select(s=>s.Item);
        
        public IEnumerable<T> Range(T from, T to, bool reverse)
        {
            var n = FindRange(from, to);
            return n == null ? Enumerable.Empty<T>() : Traverse(n, reverse).Select(s=>s.Item);
        }
        
        public void Clear()
        {
            _lock.EnterWriteLock();
            try
            {
                _root = null;
                Count = 0;
                ++_version;
            }
            finally
            {
                _lock.ExitWriteLock();
            }
        }
        
        
        public bool Add(T item)
        {
            _lock.EnterWriteLock();
            try
            {
                if (_root == null)
                {
                    // The tree is empty and this is the first item.
                    _root = new Node<T>(item, NodeColor.Black);
                    Count = 1;
                    _version++;
                    return true;
                }

                // Search for a node at bottom to insert the new node.
                // If we can guarantee the node we found is not a 4-node, it would be easy to do insertion.
                // We split 4-nodes along the search path.
                Node<T>? current = _root;
                Node<T>? parent = null;
                Node<T>? grandParent = null;
                Node<T>? greatGrandParent = null;

                // Even if we don't actually add to the set, we may be altering its structure (by doing rotations and such).
                // So update `_version` to disable any instances of Enumerator/TreeSubSet from working on it.
                _version++;

                var order = 0;
                while (current != null)
                {
                    order = _comparer.Compare(item, current.Item);
                    if (order == 0)
                    {
                        // We could have changed root node to red during the search process.
                        // We need to set it to black before we return.
                        _root.ColorBlack();
                        return false;
                    }

                    // Split a 4-node into two 2-nodes.
                    if (current.Is4Node)
                    {
                        current.Split4Node();
                        // We could have introduced two consecutive red nodes after split. Fix that by rotation.
                        if (Node<T>.IsNonNullRed(parent))
                        {
                            var notNullParent = parent!; //resharper has issues with ! suffix on ref params
                            InsertionBalance(current, ref notNullParent, grandParent!, greatGrandParent!);
                        }
                    }

                    greatGrandParent = grandParent;
                    grandParent = parent;
                    parent = current;
                    current = order < 0 ? current.Left : current.Right;
                }

                Debug.Assert(parent != null);
                // We're ready to insert the new node.
                Node<T> node = new Node<T>(item, NodeColor.Red);
                if (order > 0)
                {
                    parent.Right = node;
                }
                else
                {
                    parent.Left = node;
                }

                // The new node will be red, so we will need to adjust colors if its parent is also red.
                if (parent.IsRed)
                {
                    InsertionBalance(node, ref parent, grandParent!, greatGrandParent!);
                }

                // The root node is always black.
                _root.ColorBlack();
                ++Count;
                return true;
            }
            finally
            {
                _lock.ExitWriteLock();
            }
        }

        public bool Remove(T item, out T removed)
        {
            _lock.EnterWriteLock();
            try
            {
                removed = default(T)!;
                if (_root == null)
                {
                    return false;
                }

                // Search for a node and then find its successor.
                // Then copy the item from the successor to the matching node, and delete the successor.
                // If a node doesn't have a successor, we can replace it with its left child (if not empty),
                // or delete the matching node.
                //
                // In top-down implementation, it is important to make sure the node to be deleted is not a 2-node.
                // Following code will make sure the node on the path is not a 2-node.

                // Even if we don't actually remove from the set, we may be altering its structure (by doing rotations
                // and such). So update our version to disable any enumerators/subsets working on it.
                _version++;

                Node<T>? current = _root;
                Node<T>? parent = null;
                Node<T>? grandParent = null;
                Node<T>? match = null;
                Node<T>? parentOfMatch = null;
                var foundMatch = false;
                while (current != null)
                {
                    if (current.Is2Node)
                    {
                        // Fix up 2-node
                        if (parent == null)
                        {
                            // `current` is the root. Mark it red.
                            current.ColorRed();
                        }
                        else
                        {
                            Node<T> sibling = parent.GetSibling(current);
                            if (sibling.IsRed)
                            {
                                // If parent is a 3-node, flip the orientation of the red link.
                                // We can achieve this by a single rotation.
                                // This case is converted to one of the other cases below.
                                Debug.Assert(parent.IsBlack);
                                if (parent.Right == sibling)
                                {
                                    parent.RotateLeft();
                                }
                                else
                                {
                                    parent.RotateRight();
                                }

                                parent.ColorRed();
                                sibling.ColorBlack(); // The red parent can't have black children.
                                // `sibling` becomes the child of `grandParent` or `root` after rotation. Update the link from that node.
                                ReplaceChildOrRoot(grandParent, parent, sibling);
                                // `sibling` will become the grandparent of `current`.
                                grandParent = sibling;
                                if (parent == match)
                                {
                                    parentOfMatch = sibling;
                                }

                                sibling = parent.GetSibling(current);
                            }

                            Debug.Assert(Node<T>.IsNonNullBlack(sibling));

                            if (sibling.Is2Node)
                            {
                                parent.Merge2Nodes();
                            }
                            else
                            {
                                // `current` is a 2-node and `sibling` is either a 3-node or a 4-node.
                                // We can change the color of `current` to red by some rotation.
                                Node<T> newGrandParent = parent.Rotate(parent.GetRotation(current, sibling))!;

                                newGrandParent.Color = parent.Color;
                                parent.ColorBlack();
                                current.ColorRed();

                                ReplaceChildOrRoot(grandParent, parent, newGrandParent);
                                if (parent == match)
                                {
                                    parentOfMatch = newGrandParent;
                                }
                            }
                        }
                    }

                    // We don't need to compare after we find the match.
                    int order = foundMatch ? -1 : _comparer.Compare(item, current.Item);
                    if (order == 0)
                    {
                        // Save the matching node.
                        foundMatch = true;
                        match = current;
                        parentOfMatch = parent;
                    }

                    grandParent = parent;
                    parent = current;
                    // If we found a match, continue the search in the right sub-tree.
                    current = order < 0 ? current.Left : current.Right;
                }

                // Move successor to the matching node position and replace links.
                if (match != null)
                {
                    removed = match.Item;
                
                    ReplaceNode(match, parentOfMatch!, parent!, grandParent!);
                    --Count;
                }

                _root?.ColorBlack();
                return foundMatch;
            }
            finally
            {
                _lock.ExitWriteLock();
            }
        }
        
         /// <summary>
        /// Searches the set for a given value and returns the equal value it finds, if any.
        /// </summary>
        /// <param name="searchSurrogate">The value to search for.</param>
        /// <param name="actualValue">The value from the set that the search found, or the default value of <typeparamref name="T"/> when the search yielded no match.</param>
        /// <returns>A value indicating whether the search was successful.</returns>
        /// <remarks>
        /// This can be useful when you want to reuse a previously stored reference instead of 
        /// a newly constructed one (so that more sharing of references can occur) or to look up
        /// a value that has more complete data than the value you currently have, although their
        /// comparer functions indicate they are equal.
        /// </remarks>
        public bool TryGetValue(T searchSurrogate, out T actualValue) 
        {
            _lock.EnterReadLock();
            try
            {
                var node = Search(searchSurrogate);
                if (node != null)
                {
                    actualValue = node.Item;
                    return true;
                }
                actualValue = default(T)!; 
                return false;
            }
            finally
            {
                _lock.ExitReadLock();
            }
        }
        
        public T AddOrReplace(T searchSurrogate, Func<T> factoryFunction, Func<T, T> existedFunction) 
        {
            _lock.EnterWriteLock();
            try
            {
                var node = Search(searchSurrogate);
                if (node != null)
                    return node.Item = existedFunction(node.Item);
                
                var newValue = factoryFunction();
                Add(newValue);
                return newValue;
            }
            finally
            {
                _lock.ExitWriteLock();
            }
        }

        private Node<T>? FindRange(T from, T to)
        {
            _lock.EnterReadLock();
            try
            {
                var current = _root;
                while (current != null)
                {
                    if (_comparer.Compare(from, current.Item) > 0)
                    {
                        current = current.Right;
                    }
                    else
                    {
                        if (_comparer.Compare(to, current.Item) < 0)
                        {
                            current = current.Left;
                        }
                        else
                        {
                            return current;
                        }
                    }
                }
                return null;
            }
            finally
            {
                _lock.ExitReadLock();
            }
        }

        private IEnumerable<Node<T>> Traverse(Node<T>? from = null, bool reverse = false)
        {
            _lock.EnterReadLock();
            try
            {
                var version = _version;

                from ??= _root;

                if (from == null) yield break;
            
                // The maximum height of a red-black tree is 2 * log2(n+1).
                // See page 264 of "Introduction to algorithms" by "Thomas H. Cormen"
                // Note: It's not strictly necessary to provide the stack capacity, but we don't
                // want the stack to unnecessarily allocate arrays as it grows.

                var stack = new Stack<Node<T>>(2 * Log2(Count + 1));
                // ReSharper disable once SuggestVarOrType_Elsewhere
                Node<T>? node = from;
            
                while (node != null)
                {
                    var next = reverse ? node.Right : node.Left;
                    stack.Push(node);
                    node = next;
                }

                while (stack.Count != 0)
                {
                    var current = stack.Pop();
                    
                    if(_version != version)
                        throw new InvalidOperationException("Set was modified during enumeration");

                    yield return current;

                    node = reverse ? current.Left : current.Right;

                    while (node != null)
                    {
                        stack.Push(node);
                        node = reverse ? node.Right : node.Left;
                    }
                }
            }
            finally
            {
                _lock.ExitReadLock();
            }
        }
        
        // After calling InsertionBalance, we need to make sure `current` and `parent` are up-to-date.
        // It doesn't matter if we keep `grandParent` and `greatGrandParent` up-to-date, because we won't
        // need to split again in the next node.
        // By the time we need to split again, everything will be correctly set.
        private void InsertionBalance(Node<T> current, ref Node<T> parent, Node<T> grandParent, Node<T> greatGrandParent)
        {
            Debug.Assert(parent != null);
            Debug.Assert(grandParent != null);

            var parentIsOnRight = grandParent.Right == parent;
            var currentIsOnRight = parent.Right == current;

            Node<T> newChildOfGreatGrandParent;
            if (parentIsOnRight == currentIsOnRight)
            {
                // Same orientation, single rotation
                newChildOfGreatGrandParent = currentIsOnRight ? grandParent.RotateLeft() : grandParent.RotateRight();
            }
            else
            {
                // Different orientation, double rotation
                newChildOfGreatGrandParent = currentIsOnRight ? grandParent.RotateLeftRight() : grandParent.RotateRightLeft();
                // Current node now becomes the child of `greatGrandParent`
                parent = greatGrandParent;
            }

            // `grandParent` will become a child of either `parent` of `current`.
            grandParent.ColorRed();
            newChildOfGreatGrandParent.ColorBlack();

            ReplaceChildOrRoot(greatGrandParent, grandParent, newChildOfGreatGrandParent);
        }

        /// <summary>
        /// Replaces the child of a parent node, or replaces the root if the parent is <c>null</c>.
        /// </summary>
        /// <param name="parent">The (possibly <c>null</c>) parent.</param>
        /// <param name="child">The child node to replace.</param>
        /// <param name="newChild">The node to replace <paramref name="child"/> with.</param>
        private void ReplaceChildOrRoot(Node<T>? parent, Node<T> child, Node<T> newChild)
        {
            if (parent != null)
            {
                parent.ReplaceChild(child, newChild);
            }
            else
            {
                _root = newChild;
            }
        }

        /// <summary>
        /// Replaces the matching node with its successor.
        /// </summary>
        private void ReplaceNode(Node<T> match, Node<T> parentOfMatch, Node<T> successor, Node<T> parentOfSuccessor)
        {
            if (successor == match)
            {
                // This node has no successor. This can only happen if the right child of the match is null.
                successor = match.Left!;
            }
            else
            {
                successor.Right?.ColorBlack();

                if (parentOfSuccessor != match)
                {
                    // Detach the successor from its parent and set its right child.
                    parentOfSuccessor.Left = successor.Right;
                    successor.Right = match.Right;
                }

                successor.Left = match.Left;
            }

            if (successor != null)
            {
                successor.Color = match.Color;
            }

            ReplaceChildOrRoot(parentOfMatch, match, successor!);
        }

        private Node<T>? Search(T item)
        {
            var current = _root;
            while (current != null)
            {
                var order = _comparer.Compare(item, current.Item);
                if (order == 0)
                {
                    return current;
                }

                current = order < 0 ? current.Left : current.Right;
            }

            return null;
        }
       
        private static int Log2(int value)
        {
            var result = 0;
            while (value > 0)
            {
                result++;
                value >>= 1;
            }
            return result;
        }
    }
}
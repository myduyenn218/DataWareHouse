public interface ISearchAlgo {
	public void execute(Node tree); // traversal all nodes in the tree
	
	public Node execute(Node tree, String goal);// find the path from root node to the goal node

	public Node execute(Node tree, String start, String goal); // find the path from start node to the goal node
}

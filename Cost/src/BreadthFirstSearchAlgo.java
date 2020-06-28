import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public class BreadthFirstSearchAlgo implements ISearchAlgo {

	@Override
	public void execute(Node tree) {

		// TODO Auto-generated method stub

		LinkedList<Node> frontier = new LinkedList<Node>();
		frontier.add(tree);
		ArrayList<Node> explprered = new ArrayList<Node>();
		System.out.println("start");
		while (!frontier.isEmpty()) {
			Node node = frontier.pop();
			System.out.print(node.getLabel() + "\t");
			explprered.add(node);

			List<Node> childNode = node.getChildrenNodes();
			Collections.sort(childNode);

			for (Node child : childNode) {
				if (!explprered.contains(child) && !frontier.contains(child)) {
					frontier.add(child);
				}
			}

		}

	}

	@Override
	public Node execute(Node tree, String goal) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Node execute(Node tree, String start, String goal) {
		// TODO Auto-generated method stub
		return null;
	}

}

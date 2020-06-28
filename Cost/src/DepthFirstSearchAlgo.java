import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Stack;

public class DepthFirstSearchAlgo implements ISearchAlgo {

	@Override
	public void execute(Node tree) {
		Stack<Node> frontier = new Stack<Node>();

		frontier.add(tree);
		ArrayList<Node> explprered = new ArrayList<Node>();

		System.out.println("start");
		while (!frontier.isEmpty()) {
			Node node = frontier.pop();
			System.out.print(node.getLabel() + "\t");
			explprered.add(node);

			List<Node> childNode = node.getChildrenNodes();
			Collections.sort(childNode);
			Collections.reverse(childNode);

			for (Node child : childNode) {
				if (!explprered.contains(child) && !frontier.contains(child)) {
					frontier.push(child);
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
		Stack<Node> frontier = new Stack<Node>();

		frontier.add(tree);
		ArrayList<Node> explprered = new ArrayList<Node>();

		System.out.println("start");
		while (!frontier.isEmpty()) {
			Node node = frontier.pop();
			System.out.print(node.getLabel() + "\t");
			explprered.add(node);

			List<Node> childNode = node.getChildrenNodes();
			Collections.sort(childNode);
			Collections.reverse(childNode);

			for (Node child : childNode) {
				if (!explprered.contains(child) && !frontier.contains(child)) {
					frontier.push(child);
				}
			}
		}
	}

}

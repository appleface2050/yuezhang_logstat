package com.zsmj.inputformat;

import java.util.HashMap;

public class Trie {
	protected class Node {
		public Node parent = null;
		public String key = null;
		public HashMap<String,Node> nodes = null;
		public String value = null;
		public Node(Node parent, String key, HashMap<String,Node> nodes, String value){
			this.parent = parent;
			this.key = key;
			this.nodes = nodes;
			this.value = value;
		}
		public Node descend(String key){
			Node n = this;
			return n;
		}
	}
}

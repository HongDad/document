package com.zz.lesson04;

import java.util.ArrayList;
import java.util.List;

public class WhioutCompsitePattern {
	
	public static void main(String[] args) {
		 Department coreDep = new Department("主部门");

	        Department subDep1 = new Department("子部门1");
	        Department subDep2 = new Department("子部门2");

	        Department leafDep1 = new Department("叶子部门1");
	        Department leafDep2 = new Department("叶子部门2");
	        Department leafDep3 = new Department("叶子部门3");

	        subDep1.children.add(leafDep1);
	        subDep1.children.add(leafDep2);

	        subDep2.children.add(leafDep3);

	        coreDep.children.add(subDep1);
	        coreDep.children.add(subDep2);
	        if(coreDep.children.size() > 0) {
	        	for(Department dep:coreDep.children) {
	        		if(dep.children.size() > 0) {
	        			for(Department d:dep.children) {
	        				d.remove();
	        			}
	        		}
	        		dep.remove();
	        	}
	        }
	        coreDep.remove();
	        
	}
	
	
	public static class Department{
		private String name;
		private List<Department> children=new ArrayList<Department>();

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}
		
		public List<Department> getChildren() {
			return children;
		}

		public void setChildren(List<Department> children) {
			this.children = children;
		}

		public Department(String name) {
			this.name=name;
		}
		/**
		 * 删除部门
		 */
		public void remove() {
			System.out.println("删除："+name);
		}
		
		
	}
	

}

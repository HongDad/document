package com.zz.lesson02;
/**
 * 在业务代码里面写进去了很多非业务的代码
 * @author Administrator
 *
 */
public class WhioutBuilder {
	
	public static void main(String[] args) {
		Student student = new Student();
		if(true) {
			System.out.println("非常复杂的逻辑");
			student.setField1("test1");
		}
		if(true) {
			System.out.println("非常复杂的逻辑");
			student.setField2("tset2");
		}
		student.setField3("test3");
		
		System.out.println(student);
		
	}
	
	public static class Student{
		private String field1;
		private String field2;
		private String field3;
		public String getField1() {
			return field1;
		}
		public void setField1(String field1) {
			this.field1 = field1;
		}
		public String getField2() {
			return field2;
		}
		public void setField2(String field2) {
			this.field2 = field2;
		}
		public String getField3() {
			return field3;
		}
		public void setField3(String field3) {
			this.field3 = field3;
		}
		@Override
		public String toString() {
			return "Student [field1=" + field1 + ", field2=" + field2 + ", field3=" + field3 + "]";
		}
		
		
	}
	

}

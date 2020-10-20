package com.zz.lesson02;

import com.zz.lesson02.BuidlerPattern.Student;

public class BuidlerPattern {
	
	public static void main(String[] args) {
		Student student = new ConCreateStudent()
		.setField1("test1")
		.setField2("test2")
		.setField3("test3")
		.build();
		
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
	
	public interface Buidler{
		Buidler  setField1(String fields1);
		Buidler  setField2(String fields2);
		Buidler  setField3(String fields3);
		Student build();	
	}
	public static class ConCreateStudent implements Buidler {
		Student student=new Student();

		@Override
		public Buidler setField1(String fields1) {
			System.out.println("�����߼�");
			student.setField1(fields1);
			return this;
		}

		@Override
		public Buidler setField2(String fields2) {
			System.out.println("�����߼�");
			student.setField2(fields2);
			return this;
		}

		@Override
		public Buidler setField3(String fields3) {
			System.out.println("�����߼�");
			student.setField3(fields3);
			return this;
		}

		@Override
		public Student build() {
			
			return student;
		}
		
		
	}
	

}

package com.zz.lesson03;

import com.zz.lesson03.DecoratePattern.SuperPerson;

public class DecoratePattern {
	
	public static void main(String[] args) {
	
		SuperPerson superPerson = new SuperPerson(new Person());
		superPerson.superEat();
	}
	
	public static class Person{
		public void eat() {
			System.out.println("�Է�");
		}
	}
	public static class SuperPerson{
		private Person person;
		public SuperPerson(Person person) {
			this.person=person;
		}
		public void superEat() {
			System.out.println("�Է�������ǿ");
			this.person.eat();
			System.out.println("�Է�������ǿ");
		}
	}

}

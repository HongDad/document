package com.zz.lesson01;
/**
 * 主要体现面向对象：
 * 1）封装
 * 2）可扩展
 * @author Administrator
 *
 */
public class CommandPattern {
	
	public static void main(String[] args) {
		Context context = new Context(new ReadCommand());
		context.invoker();
	}
	
	
	
	public interface Command{
		void execute();
	}
	public static class ReadCommand implements Command{

		@Override
		public void execute() {
			System.out.println("执行读操作");
			
		}
		
	}
	public static class WriteCommand implements Command{

		@Override
		public void execute() {
			System.out.println("执行写操作");
			
		}
		
	}

	public static class Context{
		private Command command;
		public Context(Command command) {
			this.command=command;
		}
		/**
		 * 执行操作
		 */
		public void invoker() {
			this.command.execute();
		}
	}

}

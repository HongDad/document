package com.zz.lesson01;
/**
 * ��Ҫ�����������
 * 1����װ
 * 2������չ
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
			System.out.println("ִ�ж�����");
			
		}
		
	}
	public static class WriteCommand implements Command{

		@Override
		public void execute() {
			System.out.println("ִ��д����");
			
		}
		
	}

	public static class Context{
		private Command command;
		public Context(Command command) {
			this.command=command;
		}
		/**
		 * ִ�в���
		 */
		public void invoker() {
			this.command.execute();
		}
	}

}

package com.zz.lesson01;

public class WhioutCommand {
	public static void main(String[] args) {
		int style=0;
		if(style == 0) {
			Utils.reamove();
		}
		if(style == 1) {
			Utils.doCopy();
		}
	}
	
	public static class Utils{
		public static void reamove() {
			System.out.println("ִ���ƶ�����");
		}
		public static void doCopy() {
			System.out.println("ִ�и��Ʋ���");
		}
	}

}

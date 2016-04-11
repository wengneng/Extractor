package com.pactera.edg.am.metamanager.extractor.control;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.security.Security;

import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.BeanDefinitionStoreException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.remoting.rmi.RmiServiceExporter;

import com.pactera.edg.am.metamanager.extractor.util.Constants;
import com.pactera.edg.am.metamanager.extractor.util.Log4jInit;

/**
 * 开启RMI Server,单例实现,同时作同步(避免多线程开启)
 * 
 * @author user
 * @version 1.0
 * 
 */
public class RMIServerStarter {
	private final static String RMISERVER_URL = "rmi://127.0.0.1:9999/extractorService";
	
	// Window操作系统的名称
	private final static String WINDOW_OS_NAME = "WINDOWS";

	private static boolean isStarted;

	private RmiServiceExporter exporter;

	static {
		Security.addProvider(new BouncyCastleProvider());
	}

	/**
	 * 启动采集模块的RMI服务
	 * 
	 * @return 如启动成功,则返回true,否则返回false
	 */
	public boolean startRMIServer() {
		Log4jInit.init();
		if (!isStarted) {
			try {
				ApplicationContext context = new ClassPathXmlApplicationContext(Constants.RMI_SERVER_CONFIG_PATH);
				exporter = (RmiServiceExporter) context.getBean("rmiDataEngineService");
				isStarted = true;
			}
			catch (BeanDefinitionStoreException bse) {
				// 没有找到文件
				bse.printStackTrace();
			}
			catch (BeanCreationException ce) {
				// BEAN创建失败,即RMI服务没有启动
				ce.printStackTrace();
			}

		}
		return isStarted;
	}

	private boolean checkRMIServerStatus() {
		// 首先判断采集模块RMI是否已经启动
		try {
			Remote remote = Naming.lookup(RMISERVER_URL);
			if (remote != null) { return true; }
		}
		catch (MalformedURLException e) {
		}
		catch (RemoteException e) {
		}
		catch (NotBoundException e) {
		}
		return false;
	}

	public static boolean isStarted() {
		return isStarted;
	}

	/**
	 * 打印WINDOW系列下,停止采集模块的帮助信息
	 */
	private void printInfoInWindow() {
		System.out.println("------------------------------------");
		System.out.println("|  停止采集模块,请键入stop   |");
	}
	
	/**
	 * 打印非WINDOW系列下,停止采集模块的帮助信息
	 */
	private void printInfoInNonWindow() {
		System.out.println("------------------------------------");
		System.out.println("|  停止采集模块,请输入命令:  kill -9 `ps -ef|grep 'RMIServerStarter'|awk '{print $2}'`   |");
	}

	/**
	 * 关闭RMI服务的监视器,当监视到输入stop,则关闭RMI服务
	 * 适用于WINDOW系列的系统
	 *
	 * @author user
	 * @version 1.0  Date: Apr 28, 2010
	 *
	 */
	class RMIServerStopper implements Runnable {

		public void run() {
			BufferedReader reader;

			while (true) {
				reader = new BufferedReader(new InputStreamReader(System.in));
				try {
					String line = reader.readLine();
					if ("stop".equals(line)) {
						System.out.println("------------------");
						System.out.println("采集模块服务关闭中 ...");
						System.out.println("------------------");
						// 关闭RMI服务,然后退出
						exporter.destroy();
						try {
							Log4jInit.shutdown();
							// 延迟2秒
							Thread.sleep(2000);
						}
						catch (InterruptedException e) {
						}
						break;
					}
				}
				catch (IOException e) {
					System.out.println("转换发生错误！0");
				}
			}

		}

	}

	public void run() {
		if (checkRMIServerStatus()) {
			System.out.println("------------------------------------");
			System.out.println("采集模块服务已经启动!");
			return;
		}

		// 启动RMI服务
		if (startRMIServer()) {
			System.out.println("------------------");
			System.out.println("采集模块服务运行中 ...");
			
			if(System.getProperty("os.name").toUpperCase().indexOf(WINDOW_OS_NAME) > -1){
				// WINDOW系列的操作系统
				printInfoInWindow();
				Thread stopper = new Thread(new RMIServerStopper());
				stopper.start();
			}else{
				// 非WINDOW系列的操作系统
				printInfoInNonWindow();
			}
			
		}else{
			try {
				Log4jInit.shutdown();
				System.out.println("采集模块服务启动失败!");
				// 延迟2秒
				Thread.sleep(2000);
			}
			catch (InterruptedException e) {
			}
		}

	}

	public static void main(String[] args) {

		new RMIServerStarter().run();
	}

}

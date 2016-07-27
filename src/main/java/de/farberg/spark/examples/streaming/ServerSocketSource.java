package de.farberg.spark.examples.streaming;

import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.function.Supplier;

public class ServerSocketSource<T> {

	private static ArrayList<ServerSocketSource> serverSocketSources = new ArrayList<>();
	private static int portCounter = 8999;

	private Supplier<T> supplier;
	private Supplier<Integer> millisToSleep;
	private int localPort;
	private Thread worker;




	public ServerSocketSource(Supplier<T> supplier) {
		this(supplier, null);
	}

	public ServerSocketSource(Supplier<T> supplier, Supplier<Integer> millisToSleep) {
		this.supplier = supplier;
		this.millisToSleep = millisToSleep;

		createAndStartThread();
		addToStructure();

	}

	private void createAndStartThread(){
		this.worker = new Thread(()->{
			System.out.println("Initialize sender");
			try{
				ServerSocket serverSocket = new ServerSocket(getCurrentPortCounter());
				this.localPort = serverSocket.getLocalPort();
				System.out.println(serverSocket.toString());

					Socket clientSocket = serverSocket.accept();
					System.out.println("Start sending!");

					PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);

					while (true) {
						T x = supplier.get();
						if (x == null) {
							out.flush();
							out.close();
							break;
						}
						//System.out.println("Message: " +x);
						out.println(x);
						out.flush();
						if (millisToSleep != null)
							Thread.sleep(millisToSleep.get());
					}

			} catch (Exception e) {
				//System.out.println("Something went wrong");
				//e.printStackTrace();
			}

		});
		worker.start();
	}

	public synchronized void addToStructure(){
		serverSocketSources.add(this);
	}

	public synchronized void start(){
		if(!isRunning()){
			createAndStartThread();
		}
	}

	public synchronized void stop(){
		if(isRunning()){
			this.worker=null;
		}
	}

	public synchronized int getCurrentPortCounter(){
		portCounter+=1;
		return portCounter;
	}


	public synchronized boolean isRunning(){
		return this.worker!=null ;
	}

	public int getLocalPort(){
		return this.localPort;
	}
}

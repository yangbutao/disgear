package com.newcosoft.client;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

public class ClientStartupListener implements ServletContextListener {

	public void contextDestroyed(ServletContextEvent arg0) {
		// TODO Auto-generated method stub

	}

	public void contextInitialized(ServletContextEvent arg0) {
		try {
			//OssConfig osConfig = OssConfig.getInstance();
			//String zkUrl = osConfig.getProperties().getProperty("cache_zk_url");
			String zkUrl=arg0.getServletContext().getInitParameter("zkUrl");
			ClusterStateCacheManager.INSTANCE.init(zkUrl);
			ClusterStateCacheManager.INSTANCE.createClusterStateWatcher();
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		System.out.println(ClusterStateCacheManager.INSTANCE.getClusterState());

	}

}

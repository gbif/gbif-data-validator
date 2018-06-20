package org.gbif.validation.ws;

import org.gbif.utils.file.FileUtils;
import org.gbif.ws.app.Application;
import org.junit.Ignore;
import org.junit.Test;

public class WsStartupAppTest {

  /**
   * Test the startup of the webapp.  Needs to be stopped manually.
   *
   * Principally, that it doesn't fail to register with ZooKeeper.
   */
  @Test
  @Ignore
  public void testWsStartup() {
    Application.main(new String[]{
       "-conf", FileUtils.getClasspathFile("validation.properties").getPath(),
       "-host", "localhost",
       "-httpAdminPort", "7000",
       "-httpPort", "7001",
       "-stopSecret", "stop",
       "-zkHost", "c3zk1.gbif-dev.org,c3zk2.gbif-dev.org,c3zk3.gbif-dev.org",
       "-zkPath", "dev/services"
    });

  }
}

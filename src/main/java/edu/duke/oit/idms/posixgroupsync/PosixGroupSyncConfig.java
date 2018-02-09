package edu.duke.oit.idms.posixgroupsync;

import edu.duke.oit.idms.scheduled_tasks.cfg.ScheduledTaskConfig;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
    /**
     Created by rch17 on 7/26/16.
     */
    public class PosixGroupSyncConfig extends ScheduledTaskConfig {
        private static final Log LOG = LogFactory.getLog(PosixGroupSyncConfig.class);
        private static PosixGroupSyncConfig instance = null;

        private PosixGroupSyncConfig() {
            super();
            LOG.info("Done loading configuration.");
        }
        /**
         @return config */ public synchronized static PosixGroupSyncConfig getInstance() { if (instance == null) {
            instance = new PosixGroupSyncConfig();
        } return instance; }
    }

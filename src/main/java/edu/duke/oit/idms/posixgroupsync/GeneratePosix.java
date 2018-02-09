package edu.duke.oit.idms.posixgroupsync;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

import edu.duke.oit.idms.idmws.client.Result;
import edu.duke.oit.idms.idmws.client.User;
import edu.duke.oit.idms.idmws.client.UserMethods;
import edu.duke.oit.idms.idmws.client.query.QueryAnd;
import edu.duke.oit.idms.idmws.client.query.QueryComparison;
import edu.duke.oit.idms.idmws.client.query.QueryItem;
import edu.duke.oit.idms.idmws.client.query.QueryOr;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import edu.duke.oit.idms.idmws.client.dbconn.DatabaseConnectionFactory;
import edu.duke.oit.idms.scheduled_tasks.AbstractScheduledTask;


/**
 * Task that assigns POSIX attributes to accounts in IdM without them.
 * It checks IdM for active NetID accounts without a UID Number. For any found, it generates POSIX attributes and
 * assigns them to the user via the IdM web service
 *
 * The POSIX attributes assigned are:
 * USR_UDF_UIDNUMBER using a Sequence stored by the NetID DB
 * USR_UDF_GIDNUMBER is set to "500"
 * USR_UDF_LOGINSHELL is set to "/bin/bash"
 * USR_UDF_HOMEDIRECTORY is set to "deprovisioned"
 */

public class GeneratePosix extends AbstractScheduledTask {

    private static final Log LOG = LogFactory.getLog(GeneratePosix.class);

    public void run() {
        LOG.info("Running scheduled task, GeneratePosix.");


        //Quick check before we start to see if the shutdown signal has been given.
        if (super.shouldShutdown()) {
            return;
        }

        //Begin processing...

        List<User> idMUsers = this.getUsersRequiringPosix();
        LOG.info("Assigning POSIX attributes directory to IdM. count="+idMUsers.size());
        this.setUserPosixAttributes(idMUsers);

        LOG.info("Successfully finished running scheduled task, GeneratePosix.");
    }

    /**
     * This communicates with the IdM Client Web services to get a list of active IdM users that do not have a uid
     * number set (USR_UDF_UIDNUMBER == null).
     *
     * @return List of IdM Users that do not have a uid number in IdM.
     */
    private List<User> getUsersRequiringPosix(){

        LOG.info("Getting DukeIDs of accounts that need POSIX attributes from IdM.");
        ArrayList<User> userDukeIDs = new ArrayList<>();
        String[] AttributesFromIdm = new String[]{"USR_LOGIN"};

        QueryAnd query = new QueryAnd().addQuery(new QueryOr()
                .addQuery(new QueryItem("USR_UDF_ENTRYTYPE", QueryComparison.EQUALS, "people"))
                .addQuery(new QueryItem("USR_UDF_ENTRYTYPE", QueryComparison.EQUALS, "accounts"))
                .addQuery(new QueryItem("USR_UDF_ENTRYTYPE", QueryComparison.EQUALS, "test"))
        ).addQuery(new QueryAnd()
                .addQuery(new QueryItem("USR_STATUS", QueryComparison.EQUALS, "Active"))
                .addQuery(new QueryItem("USR_UDF_NETIDSTATUS", QueryComparison.NOT_LIKE, "inactive"))
                .addQuery(new QueryItem("USR_UDF_NETIDSTATUS", QueryComparison.NOT_NULL, null))
                .addQuery(new QueryItem("USR_UDF_UIDNUMBER", QueryComparison.NULL, null))
        );

        Result dbResults = UserMethods.findByQueryAsSuperAdmin(query, AttributesFromIdm, true);

        if(dbResults.getError()){
            throw new RuntimeException("Problem encountered getting list of IdM users needing POSIX attributes. " +
                    "errorMessage: " + dbResults.getErrorMessage());
        }

        userDukeIDs.addAll(dbResults.getUserQueryResult().getUsers());

        LOG.info("Found " +userDukeIDs.size()+ " accounts that need POSIX attributes in IdM.");

        return userDukeIDs;
    }

    /**
     * This takes a list of IdM Users and sets the initial values of the four POSIXs attributes:
     *
     * USR_UDF_UIDNUMBER is set to the next number in the sequence stored by the NetID DB
     * USR_UDF_GIDNUMBER is set to "500"
     * USR_UDF_LOGINSHELL is set to "/bin/bash"
     * USR_UDF_HOMEDIRECTORY is set to "deprovisioned"
     *
     * @param idMUsers is the list of IdMUsers need POSIX attributes set.
     */
    private void setUserPosixAttributes(List<User> idMUsers){

        Map<String, List<String>> attributes = new HashMap<>();

        String uidNumber;

        for(User idMUser : idMUsers) {
            //Pause to make sure we haven't cause a shutdown signal...
            if (super.shouldShutdown()) {
                return;
            }

            try {
                uidNumber = GeneratePosix.getNextSequence();
                //Setting the initial values for POSIX for a user
                attributes.put("USR_UDF_UIDNUMBER", Collections.singletonList(uidNumber));
                attributes.put("USR_UDF_GIDNUMBER", Collections.singletonList("500"));
                attributes.put("USR_UDF_LOGINSHELL", Collections.singletonList("/bin/bash"));
                attributes.put("USR_UDF_HOMEDIRECTORY", Collections.singletonList("deprovisioned"));
                attributes.put("USR_ROWVER",  Collections.singletonList(
                        idMUser.getStringValue("USR_ROWVER",true)));


                LOG.info("Setting POSIX attributes for user. dukeId=" + idMUser.getUserId() + " uidNumber="+uidNumber);

                UserMethods.updateAsAdmin(idMUser.getUserId(), attributes,  true);

            } catch (Exception e ){
                LOG.warn("Failed setting POSIX attributes for user due to exception. dukeId="+idMUser.getUserId(), e);
            }

        }

        LOG.info("All accounts have had POSIX attributes assigned.");

    }

    /**
     * This returns the next uidNumber using a Sequence from the from the NetID Database.
     *
     * @throws RuntimeException if there is no result from the Sequence call or a problem with communicating with the DB
     * @return next sequence for uidNumber as a String
     */
    private static String getNextSequence()  {
        PreparedStatement ps = null;
        ResultSet rs = null;
        Connection conn = null;

        try {
            conn = DatabaseConnectionFactory.getNetIDDatabaseConnection();
            ps = conn.prepareStatement("select uidnumber_seq.nextval from dual");
            rs = ps.executeQuery();

            if (!rs.next()) {
                //There should be at least 1 row returned.
                throw new RuntimeException("No rows returned when asked for the next uidNumber.");
            }

            return Long.toString(rs.getLong(1));

        } catch (SQLException e) {

            throw new RuntimeException("SQL Exception caught when requesting next sequence number from NetID DB.", e);

        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    LOG.warn("Failure closing ResultSet");
                }
            }

            if (ps != null) {
                try {
                    ps.close();
                } catch (SQLException e) {
                    LOG.warn("Failure closing PreparedStatement");
                }
            }

            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    LOG.warn("Failure closing SQL connection");
                }
            }
        }
    }
}
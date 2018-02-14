package edu.duke.oit.idms.posixgroupsync;

import com.mchange.v1.util.ArrayUtils;
import edu.duke.oit.idms.idmws.client.dbconn.DatabaseConnection;
import edu.duke.oit.idms.idmws.client.dbconn.DatabaseConnectionFactory;
import edu.duke.oit.idms.idmws.client.dbconn.NetIDDatabaseConnection;
import edu.duke.oit.idms.idmws.client.ldapconn.LDAPConnectionFactory;
import edu.duke.oit.idms.idmws.client.util.LDAPUtils;
import edu.duke.oit.idms.scheduled_tasks.AbstractScheduledTask;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;

import javax.naming.Context;
import javax.naming.NamingException;
import javax.naming.directory.*;
import javax.naming.ldap.LdapContext;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class PosixGroupSync extends AbstractScheduledTask {


    private static final Log LOG = LogFactory.getLog(PosixGroupSync.class);
    private static Connection conn;

    //get ou names
    @Override
    public void run() {
        LOG.info("Running Scheduled Task, PossixGroupSync");

        if (super.shouldShutdown()) {
            return;
        }

        PosixGroupSyncConfig config = PosixGroupSyncConfig.getInstance();
        String[] AttributesFromIdm = config.getProperty("baseDns", true).split("|");
        String sqlQuery = config.getProperty("sqlQuery", true);
        LdapContext context = PosixGroupSync.getContext();
        try {
        conn = DatabaseConnectionFactory.getNetIDDatabaseConnection();}
        catch (Exception e){
            LOG.warn("Could not connect to DB.", e);
        }


        if ((!org.apache.commons.lang3.ArrayUtils.isEmpty(AttributesFromIdm) && context == null)) {
            //store each dn from config file by looping through attributesfromid array
            for (int i = 0; i < AttributesFromIdm.length; i++) {
                String dn = AttributesFromIdm[i];
                //pass string dn as parameter to each iteration of ldapconnection
                List<SearchResult> results = SearchLDAP(dn, context);

                if (results != null && !results.isEmpty()) {
                    String gid = PosixGroupSync.getGidNumber(sqlQuery, conn);
                    PosixGroupSync.setGidSetPosix(results, gid, context);
                }
            }
        }
    }

    static LdapContext getContext() {
        LdapContext context = null;
        try {
            context = LDAPConnectionFactory.getAuthdirAdminConnection();
        } catch (Exception e) {
            LOG.warn("Could not connect to LDAP.", e);
        }

        return context;

    }

    static List<SearchResult> SearchLDAP(String dn, LdapContext context){

        try {
            List<SearchResult> results = LDAPUtils.findEntriesByFilter(context, dn, "(&(objectClass=groupOfNames)(!(objectClass=posixGroup)))", -1, new String[]{"cn", "objectClass"}, true);
            return results;
        } catch (Exception e) {
            LOG.warn("Could not search LDAP.", e);
            return null;
        }
    }

    static void setGidSetPosix(List < SearchResult > results, String gid, LdapContext context) {

        for (SearchResult result : results) {

            Attribute objectClass = result.getAttributes().get("objectClass");

            objectClass.add("posixGroup");

            Attributes posixAtts = new BasicAttributes();
            Attribute gidAtts = new BasicAttribute("gidNumber");
            posixAtts.put(objectClass);
            gidAtts.add(gid);
            posixAtts.put(gidAtts);
            try {
                LDAPUtils.addAttributes(context, result.getNameInNamespace(), posixAtts);

            } catch (Exception e) {
                throw new RuntimeException(e);
            } finally {
                if (context != null) {
                    try {
                        context.close();
                    } catch (Exception e) {
                        LOG.warn("WHY ARE YOU HERE IN THE CATCH BLOCK OF SETGIDSETPOSIX", e);
                    }
                }
            }
        }
    }


            /**
             * This method will take a Connection object to a database and String of the query to be used against the connection
             * to return a Sequence number that will be used as a gidNumber.
             *
             * It will take the given query and use that to create a PreparedStatement.
             * Then it will execute the PreparedStatement to get a ResultSet. That should contain a single entry that will be a
             * Long. This Long will be gathered, converted to a String and returned as the gidNumber.
             *
             * This does not handle closing the database connection, but it will close and clean up the PreparedStatement
             * and ResultSet is created.
             *
             * param conn is a Connection to the database containing the Sequence of gidNumber
             * param queryForGidNumber is the SQL query needed to get the next gidNumber from the Sequence
             * @return String representation of the next gidNumber
             */
            static String getGidNumber(String queryForGidNumber, Connection conn){
                PreparedStatement ps = null;
                ResultSet rs = null;


                try {

                    ps = conn.prepareStatement(queryForGidNumber);
                    rs = ps.executeQuery();


                    if (!rs.next()) {
                        //There should be at least 1 row returned.
                        throw new RuntimeException("No rows returned when asked for the next gidNumber.");
                    }

                    return Long.toString(rs.getLong(1));

                } catch (SQLException e) {

                    throw new RuntimeException("SQL Exception caught when requesting next sequence number from NetID DB.", e);

                } finally {
                    if (rs != null) {
                        try {
                            rs.close();
                        } catch (SQLException e) {
                            LOG.warn("Failure closing ResultSet", e);
                        }
                    }

                    if (ps != null) {
                        try {
                            ps.close();
                        } catch (SQLException e) {
                            LOG.warn("Failure closing PreparedStatement", e);
                        }
                    }

                    if (conn != null) {
                        try {
                            conn.close();
                        } catch (SQLException e) {
                            LOG.warn("Failure closing SQL connection", e);
                        }
                    }
                }
            }


            /**
             * This method will take an LdapContext object to search against, a String baseDN of where to start the search, and
             * a optional array of attributes to return.
             *
             * It will return all groups under the given baseDN that have objectClass=groupOfNames but do not have
             * objectClass=posixGroup. It will also return any attributes provided.
             *
             * This will not close the LdapContext.
             *
             * @param context the LdapContext to search
             * @param baseDN the start point of the of the search
             * @param attributes optional String[] of attributes to return with the results
             * @return List of SearchResults for all the groups that match with any requested attributes
             */
            static List<SearchResult> getLDAPGroupsToSync (LdapContext context, String baseDN, String ...attributes){
                List<SearchResult> searchResults = null;


                // Use you LDAPUtils here:
                // searchResults = LDAPUtils.findEntriesByFilter(........)

                try {
                    searchResults = LDAPUtils.findEntriesByFilter(context, baseDN, "(&(objectClass=groupOfNames)(!(objectClass=posixGroup)))", -1, new String[]{"cn"}, true);
                } catch (NamingException e) {
                    System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!11");
                }
                return searchResults;
            }

        }


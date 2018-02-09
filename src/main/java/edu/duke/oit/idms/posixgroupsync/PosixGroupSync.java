package edu.duke.oit.idms.posixgroupsync;

import edu.duke.oit.idms.idmws.client.dbconn.DatabaseConnectionFactory;
import edu.duke.oit.idms.idmws.client.ldapconn.LDAPConnectionFactory;
import edu.duke.oit.idms.idmws.client.util.LDAPUtils;
import edu.duke.oit.idms.scheduled_tasks.AbstractScheduledTask;

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

    Attributes stuffs;



    //get ou names
    @Override
    public void run() {

        PosixGroupSyncConfig config = PosixGroupSyncConfig.getInstance();
        String[] AttributesFromIdm = config.getProperty("con1", true).split("|");

        //store each dn from config file by looping through attributesfromid array
        for (int i = 0; i <AttributesFromIdm.length; i++) {
            String dn = AttributesFromIdm[i];
            //pass string dn as parameter to each iteration of ldapconnection
            connectToLdap(dn);
        }
        if (super.shouldShutdown()) {
            return;
        }

    }


    public void connectToLdap(String dn) {
        LdapContext context = null;

        try {
            PosixGroupSync gid = new PosixGroupSync();
            context = LDAPConnectionFactory.getAuthdirAdminConnection();
            List<SearchResult> results = LDAPUtils.findEntriesByFilter(context, dn, "(&(objectClass=groupOfNames)(!(objectClass=posixGroup)))", -1, new String [] {"cn"}, true );

            if(super.shouldShutdown()){
                return;
            }

            for (SearchResult result :results){
                Attribute objectClass = result.getAttributes().get("objectClass");

                objectClass.add("possixGroup");

                Attributes stuffs = new BasicAttributes();
                Attribute stuff = new BasicAttribute("gidNumber");
                stuffs.put(objectClass);
                stuff.add(gid);
                stuffs.put(stuff);

                LDAPUtils.addAttributes(context, result.getNameInNamespace(), stuffs);

            }


        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (context != null) {
                try {
                    context.close();
                } catch (Exception e) {
                    // ignore
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
     * @param conn is a Connection to the database containing the Sequence of gidNumber
     * @param queryForGidNumber is the SQL query needed to get the next gidNumber from the Sequence
     * @return String representation of the next gidNumber
     */
    static String getGidNumber(Connection conn, String queryForGidNumber){
        return "";
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
    static List<SearchResult> getLDAPGroupsToSync(LdapContext context, String baseDN, String ... attributes){
        List<SearchResult> searchResults = null;

        // Use you LDAPUtils here:
        // searchResults = LDAPUtils.findEntriesByFilter(........)

        try {
            searchResults = LDAPUtils.findEntriesByFilter(context, baseDN, "(&(objectClass=groupOfNames)(!(objectClass=posixGroup)))", -1, new String[]{"cn"}, true);
        } catch (NamingException e){
            System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!11");
        }
        return searchResults;
    }

}
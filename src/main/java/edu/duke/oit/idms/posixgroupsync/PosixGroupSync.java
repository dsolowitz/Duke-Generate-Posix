package edu.duke.oit.idms.posixgroupsync;

import edu.duke.oit.idms.idmws.client.ldapconn.LDAPConnectionFactory;
import edu.duke.oit.idms.idmws.client.util.LDAPUtils;
import edu.duke.oit.idms.scheduled_tasks.AbstractScheduledTask;

import javax.naming.directory.*;
import javax.naming.ldap.LdapContext;
import java.util.List;

public class PosixGroupSync extends AbstractScheduledTask {

    Attributes stuffs;



    private static final PosixGroupSyncConfig config = PosixGroupSyncConfig.getInstance().getInstance();
    //get ou names
    String[] AttributesFromIdm = config.getProperty("con1", true).split("|");
    @Override
    public void run() {

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




}
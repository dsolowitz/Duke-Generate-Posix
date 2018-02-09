package edu.duke.oit.idms.posixgroupsync.test.utils;

import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class MockDBConnection {

    public  Connection getMockDBConnection(){
        return Mockito.mock(Connection.class);
    }

    public  Connection getMockDBConnection(String query, long result) throws SQLException{

        Connection dbConn =  Mockito.mock(Connection.class);
        PreparedStatement ps = Mockito.mock(PreparedStatement.class);
        ResultSet rs =  Mockito.mock(ResultSet.class);;

        Mockito.when(dbConn.prepareStatement(query)).thenReturn(ps);
        Mockito.when(ps.executeQuery()).thenReturn(rs);

        if(result > -1 ) {
            Mockito.when(rs.next()).thenReturn(true);
            Mockito.when(rs.getLong(1)).thenReturn(result);
        } else {
            Mockito.when(rs.next()).thenReturn(false);
        }

        return dbConn;
    }

}

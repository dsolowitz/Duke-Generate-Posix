package edu.duke.oit.idms.posixgroupsync.test.utils;

import org.mockito.Matchers;
import org.mockito.Mockito;

import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.*;
import javax.naming.ldap.LdapContext;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MockLdapContext {

    /**
     *
     * @return
     * @throws NamingException
     */
    public LdapContext getMockedLdapContext(String baseDN, String filter, List<SearchResult> searchResults) throws NamingException {

        LdapContext mockedLdapContext = Mockito.mock(LdapContext.class);
        MockedNamingEnumeration mockedNamingEnumeration = new MockedNamingEnumeration();

        mockedNamingEnumeration.loadSearchResult(searchResults);


        Mockito.when(mockedLdapContext.search(
                Mockito.eq(baseDN),
                Mockito.eq(filter),
                Mockito.any(SearchControls.class))).thenReturn(mockedNamingEnumeration);

        return mockedLdapContext;
    }


    public SearchResult getMockedSearchResult(String nameInNameSpace, Map<String, List<String>> attributes){
        SearchResult mockedSearchResult = Mockito.mock(SearchResult.class);

        Mockito.when(mockedSearchResult.getNameInNamespace()).thenReturn(nameInNameSpace);

        Attributes attributesToReturn = new BasicAttributes();
        for (String attributeName : attributes.keySet()){
            Attribute attribute = new BasicAttribute(attributeName);
            for (String attributeValue : attributes.get(attributeName)){
                attribute.add(attributeValue);
            }
            attributesToReturn.put(attribute);
        }

        Mockito.when(mockedSearchResult.getAttributes()).thenReturn(attributesToReturn);

        return mockedSearchResult;
    }




    private class MockedNamingEnumeration implements NamingEnumeration/*<SearchResult>*/ {

        private List<SearchResult> results = new ArrayList<>();

        public void close() throws NamingException {
        }

        public boolean hasMore() throws NamingException {
            return hasMoreElements();
        }

        public Object next() throws NamingException {
            return nextElement();
        }

        public boolean hasMoreElements() {
            return results.size()>0;
        }

        public Object nextElement() {

            return results.remove(0);
        }

        void loadSearchResult(List<SearchResult> resultsToAdd){
                results.addAll(resultsToAdd);
        }
    }

}

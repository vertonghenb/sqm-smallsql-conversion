package smallsql.junit;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Locale;
import java.util.Set;
import smallsql.database.language.Language;
public class TestLanguage extends BasicTestCase {
	private static final String TABLE_NAME = "test_lang";
	private static final String[] OTHER_LANGUAGES = { "it", "de" };
	public void setUp() throws SQLException {
		tearDown();
	}
	public void tearDown() throws SQLException {
		Connection conn = AllTests.createConnection("?locale=en", null);
		try {
			conn.prepareStatement("DROP TABLE " + TABLE_NAME).execute();
		}
		catch (Exception e) {}
		finally {
			conn.close();
		}
	}
	public void testBogusLocale() throws SQLException {
		Locale origLocale = Locale.getDefault();
		Locale.setDefault(Locale.ITALY);
		Connection conn = AllTests.createConnection("?locale=XXX", null);
		Statement stat = conn.createStatement();
		try {
			recreateTestTab(stat);
			stat.execute("CREATE TABLE " + TABLE_NAME + " (id_test INT)");
			fail();
		}
		catch (SQLException e) {
			assertMessage(e, "La tabella/vista '" + TABLE_NAME + "' è già esistente.");
		}
		finally {
			Locale.setDefault(origLocale);
			conn.close();
		}
	}
	public void testLocalizedErrors() throws Exception {
		Connection conn = AllTests.createConnection("?locale=it", null);
		Statement stat = conn.createStatement();
		try {
			try {
				recreateTestTab(stat);
				stat.execute("CREATE TABLE " + TABLE_NAME + " (id_test INT)");
				fail();
			}
			catch(SQLException e) {
				assertMessage(e, "La tabella/vista '" + TABLE_NAME + "' è già esistente.");
			}
			try {
				stat.execute("DROP TABLE " + TABLE_NAME);
				stat.execute("DROP TABLE " + TABLE_NAME);
			}
			catch (SQLException e) {
				assertMessage(e, "Non si può effettuare DROP della tabella");
			}
			try {
				stat.execute("CREATE TABLE foo");
			}
			catch (SQLException e) {
				assertMessage(e, "Errore di sintassi, fine inattesa");
			}
		}
		finally {
			conn.close();
		}
	}
	public void testSyntaxErrors() throws SQLException {
		Connection conn = AllTests.createConnection("?locale=it", null);
		Statement stat = conn.createStatement();
		try {
			try {
				stat.execute("CREATE TABLE");
			}
			catch (SQLException se) {
				assertMessage(se, "Errore di sintassi, fine inattesa della stringa SQL. Le parole chiave richieste sono: <identifier>");
			}
			try {
				stat.execute("Some nonsensical sentence.");
			}
			catch (SQLException se) {
				assertMessage(se, "Errore di sintassi alla posizione 0 in 'Some'. Le parole chiave richieste sono");
			}
			recreateTestTab(stat);
			try {
				stat.execute("SELECT bar() FROM foo");
			}
			catch (SQLException se) {
				assertMessage(se, "Errore di sintassi alla posizione 7 in 'bar'. Funzione sconosciuta");
			}
			try {
				stat.execute("SELECT UCASE('a', '');");
			}
			catch (SQLException se) {
				assertMessage(se, "Errore di sintassi alla posizione 7 in 'UCASE'. Totale parametri non valido.");
			}
		}
		finally {
			conn.close();
		}
	}
	private void assertMessage(SQLException e, String expectedText) {
		assertMessage(e, new String[] { expectedText });
	}
	private void assertMessage(SQLException e, String[] expectedTexts) {
		String message = e.getMessage();
		boolean found = true;
		for (int i = 0; found && i < expectedTexts.length; i++) {
			found = found && message.indexOf(expectedTexts[i]) >= 0;
		}
		if (! found) {
			System.err.println("ERROR [Wrong message]:" + message);
			fail();
		}
	}
	private void recreateTestTab(Statement stat) throws SQLException {
		stat.execute("CREATE TABLE " + TABLE_NAME + " (id_test INT)");
	}
	public void testEntries() throws Exception {
		boolean failed = false;
        StringBuffer msgBuf = new StringBuffer();
		Language eng = Language.getLanguage("en"); 
        HashSet engEntriesSet = new HashSet();
        String[][] engEntriesArr = eng.getEntries();
        for (int j = 1; j < engEntriesArr.length; j++) {
            engEntriesSet.add(engEntriesArr[j][0]);
        }
		for (int i = 0; i < OTHER_LANGUAGES.length; i++) {
			String localeStr = OTHER_LANGUAGES[i];
			Language lang2 = Language.getLanguage(localeStr);
            HashSet otherEntriesSet = new HashSet();        
            String[][] otherEntriesArr = lang2.getEntries();        
            for (int j = 0; j < otherEntriesArr.length; j++) {
                otherEntriesSet.add(otherEntriesArr[j][0]);
            }
			Set diff = (Set)engEntriesSet.clone();
            diff.removeAll(otherEntriesSet);
			if (diff.size() > 0) {
				failed = true;
                msgBuf.append("\nMissing entries for language ").append( OTHER_LANGUAGES[i] ).append(": ");
				for (Iterator itr = diff.iterator(); itr.hasNext(); ) {
					msgBuf.append(itr.next());
					if (itr.hasNext()) msgBuf.append(',');
				}
			}
            diff = (Set)otherEntriesSet.clone();
            diff.removeAll(engEntriesSet);
            if (diff.size() > 0) {
                failed = true;
                msgBuf.append("\nAdditional entries for language ").append( OTHER_LANGUAGES[i] ).append(": ");
                for (Iterator itr = diff.iterator(); itr.hasNext(); ) {
                    msgBuf.append(itr.next());
                    if (itr.hasNext()) msgBuf.append(',');
                }
            }
            StringBuffer buf = new StringBuffer();
            for (int j = 1; j < engEntriesArr.length; j++) {
                String key = engEntriesArr[j][0];
                String engValue = eng.getMessage(key);
                String otherValue = lang2.getMessage(key);
                if(engValue.equals(otherValue)){
                    failed = true;
                    if(buf.length() > 0){
                        buf.append(',');
                    }
                    buf.append(key);
                }
            }
            if(buf.length()>0){
                msgBuf.append("\nNot translated entries for language ").append( OTHER_LANGUAGES[i] ).append(": ");
                msgBuf.append(buf);
            }
		}		
		if (failed){
            System.err.println(msgBuf);
            fail(msgBuf.toString());
        }
	}
}
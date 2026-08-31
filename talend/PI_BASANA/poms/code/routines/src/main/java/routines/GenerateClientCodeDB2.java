package routines;

import java.sql.Connection;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class GenerateClientCodeDB2 {
    
    /**
     * Génère un code client unique en utilisant une séquence SQL Server
     */
    public static String generateCode(Connection conn, String nom, String prenom, String ville) 
            throws SQLException {
        
        int sequence = getNextSequenceValue(conn);
        
        String codeNom = formatPart(nom);
        String codePrenom = formatPart(prenom);
        String codeVille = formatPart(ville);
        
        return codeNom + codePrenom + codeVille + "_" + String.format("%06d", sequence);
    }
    
    /**
     * Version sécurisée qui gère les erreurs et la connexion null
     */
    public static String generateCodeSafe(Connection conn, String nom, String prenom, String ville) {
        if (conn == null) {
            System.err.println("ERREUR: Connexion null");
            return "ERR_CONN_" + System.currentTimeMillis();
        }
        
        try {
            return generateCode(conn, nom, prenom, ville);
        } catch (SQLException e) {
            e.printStackTrace();
            return "ERR_SQL_" + System.currentTimeMillis();
        } catch (Exception e) {
            e.printStackTrace();
            return "ERR_" + System.currentTimeMillis();
        }
    }
    
    /**
     * Vérifie si la séquence existe
     */
    public static boolean checkSequenceExists(Connection conn) {
        if (conn == null) {
            return false;
        }
        
        String sql = "SELECT 1 FROM sys.sequences WHERE name = 'seq_client_code'";
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            return rs.next();
        } catch (SQLException e) {
            System.err.println("Erreur vérification séquence: " + e.getMessage());
            return false;
        }
    }
    
    private static int getNextSequenceValue(Connection conn) throws SQLException {
        String sql = "SELECT NEXT VALUE FOR seq_client_code AS next_val";
        
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            
            if (rs.next()) {
                return rs.getInt("next_val");
            } else {
                throw new SQLException("Impossible de récupérer la prochaine valeur de séquence");
            }
        }
    }
    
    private static String formatPart(String value) {
        if (value == null || value.trim().isEmpty()) {
            return "XXX";
        }
        
        String cleaned = value.trim().toUpperCase();
        
        cleaned = cleaned.replaceAll("[ÀÁÂÃÄÅ]", "A")
                        .replaceAll("[àáâãäå]", "A")
                        .replaceAll("[ÈÉÊË]", "E")
                        .replaceAll("[èéêë]", "E")
                        .replaceAll("[ÌÍÎÏ]", "I")
                        .replaceAll("[ìíîï]", "I")
                        .replaceAll("[ÒÓÔÕÖ]", "O")
                        .replaceAll("[òóôõö]", "O")
                        .replaceAll("[ÙÚÛÜ]", "U")
                        .replaceAll("[ùúûü]", "U")
                        .replaceAll("[Çç]", "C")
                        .replaceAll("[^A-Z0-9]", "");
        
        if (cleaned.length() >= 3) {
            return cleaned.substring(0, 3);
        } else if (cleaned.length() > 0) {
            return cleaned + "XXX".substring(0, 3 - cleaned.length());
        } else {
            return "XXX";
        }
    }
}
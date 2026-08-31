package routines;

public class CityStandardizer {

    public static String normalizeCity(String city) {

        if (city == null || city.trim().isEmpty()) {
            return "";
        }

        String result = city.trim().toUpperCase();

        // supprimer accents
        result = result.replaceAll("[ÀÁÂÃÄÅ]", "A")
                       .replaceAll("[ÈÉÊË]", "E")
                       .replaceAll("[ÌÍÎÏ]", "I")
                       .replaceAll("[ÒÓÔÕÖ]", "O")
                       .replaceAll("[ÙÚÛÜ]", "U")
                       .replaceAll("[Ç]", "C");

        // caractères spéciaux → espace
        result = result.replaceAll("[^A-Z0-9\\s]", " ");

        // espaces multiples
        result = result.replaceAll("\\s+", " ").trim();

        return result;
    }
}
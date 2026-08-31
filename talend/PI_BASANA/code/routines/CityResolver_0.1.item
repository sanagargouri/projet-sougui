package routines;

public class CityResolver {

    public static String resolveCity(String rawCity) {

        // 1. NORMALISATION
        String norm = CityStandardizer.normalizeCity(rawCity);

        // 2. CLEAN
        String clean = CityCleaner.cleanCity(norm);

        // 3. EXTRACTION
        String extracted = CityExtractor.extractCity(clean);

        if (extracted != null) {
            return extracted;
        }

        // 4. RULE ENGINE

        if (clean.contains("SANA")) return "TUNIS";
        if (clean.contains("TUNIS")) return "TUNIS";

        if (clean.contains("ARIANA") || clean.contains("SOUKRA") || clean.contains("ENNASER"))
            return "ARIANA";

        if (clean.contains("MOUROUJ") || clean.contains("MOUR") || clean.contains("RADES") || clean.contains("EZZAHRA"))
            return "BEN_AROUS";

        if (clean.contains("LAC") || clean.contains("BERGES") || clean.contains("CARTHAGE") || clean.contains("MARSA"))
            return "TUNIS";

        if (clean.contains("SFAX")) return "SFAX";
        if (clean.contains("SOUSSE") || clean.contains("MSAKEN")) return "SOUSSE";
        if (clean.contains("NABEUL") || clean.contains("HAMMAMET")) return "NABEUL";
        if (clean.contains("BIZERTE")) return "BIZERTE";
        if (clean.contains("GABES")) return "GABES";
        if (clean.contains("MAHDIA")) return "MAHDIA";
        if (clean.contains("KAIROUAN")) return "KAIROUAN";

        // 5. FUZZY MATCH
        String fuzzy = CityFuzzy.bestMatch(clean);

        if (!fuzzy.equals("UNKNOWN")) {
            return fuzzy;
        }

        // 6. DEFAULT
        return "VILLE_INCONNUE";
    }
}
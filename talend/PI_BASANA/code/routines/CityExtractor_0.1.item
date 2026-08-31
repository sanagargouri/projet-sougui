package routines;

public class CityExtractor {

    public static String extractCity(String input) {

        if (input == null || input.isEmpty()) return null;

        String[] words = input.split(" ");

        for (int i = words.length - 1; i >= 0; i--) {

            String w = words[i];

            if (w.equals("TUNIS") ||
                w.equals("ARIANA") ||
                w.equals("SFAX") ||
                w.equals("SOUSSE") ||
                w.equals("NABEUL") ||
                w.equals("BIZERTE") ||
                w.equals("MONASTIR") ||
                w.equals("GABES") ||
                w.equals("MAHDIA") ||
                w.equals("KAIROUAN")) {

                return w;
            }
        }

        return null;
    }
}
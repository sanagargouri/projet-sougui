package routines;

public class CityCleaner {

    public static String cleanCity(String input) {

        if (input == null) return "";

        String text = input;

        // supprimer mots parasites
        text = text.replaceAll("RUE|AVENUE|AV|ROUTE|IMM|IMMEUBLE|CENTER|CENTRE|BUILDING|RESIDENCE|APPARTEMENT", " ");

        // supprimer chiffres (codes postaux)
        text = text.replaceAll("\\d+", " ");

        // nettoyer espaces
        text = text.replaceAll("\\s+", " ").trim();

        return text;
    }
}
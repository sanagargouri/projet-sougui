package routines;

public class CityFuzzy {

    public static int levenshtein(String a, String b) {

        int[][] dp = new int[a.length()+1][b.length()+1];

        for (int i = 0; i <= a.length(); i++) {
            for (int j = 0; j <= b.length(); j++) {

                if (i == 0) dp[i][j] = j;
                else if (j == 0) dp[i][j] = i;
                else {
                    dp[i][j] = Math.min(
                        Math.min(dp[i-1][j] + 1, dp[i][j-1] + 1),
                        dp[i-1][j-1] + (a.charAt(i-1) == b.charAt(j-1) ? 0 : 1)
                    );
                }
            }
        }

        return dp[a.length()][b.length()];
    }

    public static String bestMatch(String input) {

        if (input == null || input.isEmpty()) return "UNKNOWN";

        String[] cities = {
            "TUNIS","ARIANA","SFAX","SOUSSE",
            "NABEUL","BIZERTE","MONASTIR",
            "MAHDIA","GABES","KAIROUAN"
        };

        int minDistance = 999;
        String best = "UNKNOWN";

        for (String c : cities) {

            int d = levenshtein(input, c);

            if (d < minDistance && d <= 3) {
                minDistance = d;
                best = c;
            }
        }

        return best;
    }
}
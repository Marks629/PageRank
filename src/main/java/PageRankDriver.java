public class PageRankDriver {

    public static void main (String[] args) throws Exception {
        UnitMultiplication unitMultiplication = new UnitMultiplication();
        UnitSum unitSum = new UnitSum();

        // args0: dir of transitionMatrix
        // args1: dir of pageRank
        // args2: dir of subPageRank (output of UnitMultiplication job)
        // args3: times of convergence

        String transitionMatrix = args[0];
        String prMatrix = args[1];
        String subPR = args[2];
        int times = Integer.parseInt(args[3]);

        // convergence
        for (int i=0; i<times; i++) {
            String[] multiplicationArgs = {transitionMatrix, prMatrix + i, subPR + i};
            unitMultiplication.main(multiplicationArgs);

            String[] sumArgs = {subPR + i, prMatrix + (i+1)};
            unitSum.main(sumArgs);
        }
    }
}

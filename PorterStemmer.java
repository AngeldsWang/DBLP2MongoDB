/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package DBLPTopicModel;

/**
 *
 * @author zhenjun.wang
 */

import cc.mallet.pipe.Pipe;
import cc.mallet.types.Instance;
import cc.mallet.types.TokenSequence;

public class PorterStemmer extends Pipe {

    private static final long serialVersionUID = 154100332101873830L;

    public Instance pipe(Instance carrier) {
        TokenSequence ts = (TokenSequence) carrier.getData();
        String word;
        Stemmer s;

        for (int i = 0; i < ts.size(); i++) {
            word = ts.get(i).getText();
            //stem the word
            s = new Stemmer();
            for (char ch : word.toCharArray()) {
                if (Character.isLetter(ch)) {
                    s.add(ch);
                }
            }
            s.stem();
            ts.get(i).setText(s.toString());
        }
        carrier.setData(ts);

        return carrier;
    }
}
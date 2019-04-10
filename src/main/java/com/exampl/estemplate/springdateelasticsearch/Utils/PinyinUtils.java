/**
 * Copyright (C), 2015-2019
 * FileName: PinyinUtils
 * Author:   huhu
 * Date:     2019/4/9 20:57
 * Description:
 * History:
 * <author>          <time>          <version>          <desc>
 * 作者姓名           修改时间           版本号              描述
 */
package com.exampl.estemplate.springdateelasticsearch.Utils;

import net.sourceforge.pinyin4j.PinyinHelper;

/**
 * 〈一句话功能简述〉<br>
 * 〈〉
 *
 * @author huhu
 * @create 2019/4/9
 * @since 1.0.0
 */
public class PinyinUtils {
    
    /**
     * 功能描述: <br>
     * 〈将汉字转换为拼音，提取每个汉字的首字母（大写）〉
     *
     * @param str 汉字
     * @return: pinyin
     * @since: 1.0.0
     * @Author:huhu
     * @Date: 2019/4/9 21:01
     */
    public static String getPinYinHeadChar(String str) {
        String convert = "";
        if (null != str && !str.isEmpty()) {
            for (int j = 0; j < str.length(); j++) {
                char word = str.charAt(j);
                String[] pinyinArray = PinyinHelper.toHanyuPinyinStringArray(word);// 提取汉字的首字母
                if (pinyinArray != null) {
                    convert += pinyinArray[0].charAt(0);
                } else {
                    convert += word;
                }
            }
        }
        return convert.toUpperCase();
    }

    public static void main(String[] args) {
        String pinyin=getPinYinHeadChar("测试");
        System.out.println(pinyin);
    }
}
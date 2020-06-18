package com.cbp.util;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.util.Random;

public class BaseUtil {

    // 64个字符
    private final static char[] CHARS64 = {
            '0', '1', '2', '3', '4', '5',
            '6', '7', '8', '9', 'A', 'B',
            'C', 'D', 'E', 'F', 'G', 'H',
            'I', 'J', 'K', 'L', 'M', 'N',
            'O', 'P', 'Q', 'R', 'S', 'T',
            'U', 'V', 'W', 'X', 'Y', 'Z',
            'a', 'b', 'c', 'd', 'e', 'f',
            'g', 'h', 'i', 'j', 'k', 'l',
            'm', 'n', 'o', 'p', 'q', 'r',
            's', 't', 'u', 'v', 'w', 'x',
            'y', 'z', '+', '/'};

    // 32个字符，去除了ISOZ
    private final static char[] CHARS32 = {
            '0', '1', '2', '3', '4', '5',
            '6', '7', '8', '9', 'A', 'B',
            'C', 'D', 'E', 'F', 'G', 'H',
            'J', 'K', 'L', 'M', 'N', 'P',
            'Q', 'R', 'T', 'U', 'V', 'W',
            'X', 'Y',
    };

    public static void main(String[] args) throws UnsupportedEncodingException {
        // long a = 918273645012345678L;
        // long t1 = System.currentTimeMillis();
        // System.out.println(convertTo32(a));
        // long t2 = System.currentTimeMillis();
        // System.out.println(digits32(a));
        // long t3 = System.currentTimeMillis();
        // System.out.println(t2 - t1);
        //
        // System.out.println(random32Str(8));
        // long t5 = System.currentTimeMillis();
        // System.out.println(t5 - t3);
        System.out.println("中国光大银行" + md5("中国光大银行"));
//        System.out.println("中国光大银行" + DigestUtils.md5DigestAsHex(("中国光大银行").getBytes("UTF-8")));
        System.out.println("风险管理部" + md5("风险管理部"));
        System.out.println("模型" + md5("模型"));
        System.out.println("建模" + md5("建模"));
        System.out.println("数据" + md5("数据9599TA7MVH42"));
        System.out.println(getFpbm("7021622016137","021622016"));

    }

    public  static String getFpbm(String fpdm, String fphm) {
        fpdm = fpdm.length() > 10 ? fpdm.substring(fpdm.length() - 10) : fpdm;
        fpdm = fphm.length() > 8 ? fpdm.substring(fphm.length() - 8) : fphm;
        return BaseUtil.digits32(Long.valueOf(fpdm + fphm));
    }
    /**
     * long转32进制字符串,逻辑运算
     *
     * @param num
     * @return
     */
    public static String convertTo32(long num) {
        StringBuilder value = new StringBuilder();
        while (true) {
            int n = (int) (num % CHARS32.length);
            num = (num - n) / CHARS32.length;
            value.append(CHARS32[n]);
            if (num <= 0)
                break;
        }
        return value.reverse().toString();
    }


    /**
     * long类型转为32进制，指定了使用的字符，参考Long.toUnsignedString0，位移运算
     *
     * @param val
     * @return
     */
    public static String digits32(long val) {
        // 32=2^5=二进制100000
        int shift = 5;
        // numberOfLeadingZeros 获取long值从高位连续为0的个数，比如val=0，则返回64
        // 此处mag=long值二进制减去高位0之后的长度
        int mag = Long.SIZE - Long.numberOfLeadingZeros(val);
        int len = Math.max(((mag + (shift - 1)) / shift), 1);
        char[] buf = new char[len];
        do {
            // &31相当于%32
            buf[--len] = CHARS32[((int) val) & 31];
            val >>>= shift;
        } while (val != 0 && len > 0);
        return new String(buf);
    }

    /**
     * 随机生产一个32进制的字符串
     *
     * @param length 指定长度
     * @return
     */
    public static String random32Str(int length) {
        Random random = new Random();
        char[] buf = new char[length];
        for (int i = 0; i < length; i++) {
            buf[i] = CHARS32[random.nextInt(32)];
        }
        return new String(buf);
    }


    public static String md5(String dataStr) {
        try {
            MessageDigest m = MessageDigest.getInstance("MD5");
            m.update(dataStr.getBytes("UTF8"));
            byte s[] = m.digest();
            String result = "";
            for (int i = 0; i < s.length; i++) {
                result += Integer.toHexString((0x000000FF & s[i]) | 0xFFFFFF00).substring(6);
            }
            return result;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

}


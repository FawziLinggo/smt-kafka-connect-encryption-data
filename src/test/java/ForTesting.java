import java.security.SecureRandom;

import static dev.snowmancode.kafka.connect.smt.model.AESEncryptionCustom.decrypt;
import static dev.snowmancode.kafka.connect.smt.model.AESEncryptionCustom.encrypt;

public class ForTesting {
    public static void main(String[] args) throws Exception {
        String plaintext = "Data rahasia yang akan dienkripsi";
        String secretKey = "s3cr3tK3y1234569"; // Kunci rahasia harus memiliki panjang 16, 24, atau 32 byte (128, 192, atau 256 bit)

        // Generate Initialization Vector (IV) secara acak (untuk mode CBC)
        byte[] iv = new byte[16];
        SecureRandom random = new SecureRandom();
        random.nextBytes(iv);

        // Enkripsi
        String encryptedText = encrypt(plaintext, secretKey, iv);

        System.out.println("Plaintext: " + plaintext);
        System.out.println("Encrypted Text: " + encryptedText);

        // Dekripsi
        String decryptedText = decrypt(encryptedText, secretKey, iv);

        System.out.println("Decrypted Text: " + decryptedText);
    }
}

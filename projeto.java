import com.mongodb.ErrorCategory;
import com.mongodb.MongoWriteException;
import com.mongodb.client.MongoClient;
import com.mongodb.ErrorCategory;
import com.mongodb.MongoWriteException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

import java.util.Scanner;

public class Projeto_Ibge {

    public static void main(String[] args) {
        MongoClient mongoClient = MongoClients.create("mongodb://localhost:27017/");
        MongoDatabase mongoDatabase = mongoClient.getDatabase("Ibge");
        System.out.println("Conexão Estabelecida");

        Scanner scanner = new Scanner(System.in);

        while (true) {
            System.out.println("Escolha uma funcionalidade do Sistema :");
            System.out.println("1. Adição Única de Documento :");
            System.out.println("2. Adição Múltiplas de Documentos :");
            System.out.println("3. Visualização de Todos os Documentos :");
            System.out.println("4. Visualização Única:");
            System.out.println("5. Atualizar por ObjectId:");
            System.out.println("6. Deletar por ObjectId");
            System.out.println("0. Sair");
            System.out.println("Digite o Número:");

            int choose = scanner.nextInt();

            switch (choose) {
                case 1:
                    singleAddition(mongoDatabase);
                    break;
                case 2:
                    multipleAddition(mongoDatabase);
                    break;
                case 3:
                    visualizeAll(mongoDatabase);
                    break;
                case 4:
                    visualizeById(mongoDatabase);
                    break;
                case 5:
                    updateById(mongoDatabase);
                    break;
                case 6:
                    deleteById(mongoDatabase);
                    break;
                case 0:
                    System.out.println("Saindo do programa.");
                    System.exit(0);
                    break;
                default:
                    System.out.println("Opção inválida. Tente novamente.");
                    break;
            }
        }
    }

    private static void singleAddition(MongoDatabase mongoDatabase) {
        MongoCollection<Document> mongoCollection = mongoDatabase.getCollection("Estados");

        Document rioGrandeNorte = new Document();
        rioGrandeNorte.append("UF", "RN").append("nome", "Rio Grande do Norte").append("Região", "Nordeste");
        try {
            mongoCollection.insertOne(rioGrandeNorte);
            System.out.println("Inserção Concluída");
        } catch (MongoWriteException mwe) {
            if (mwe.getError().getCategory().equals(ErrorCategory.DUPLICATE_KEY)) {
                System.out.println("Document with that id already exists");
            }
        }
    }

    private static void multipleAddition(MongoDatabase mongoDatabase) {
        // Implemente conforme necessário
    }

    private static void visualizeAll(MongoDatabase mongoDatabase) {
        MongoCollection<Document> mongoCollection = mongoDatabase.getCollection("Estados");
        Bson projection = Projections.excludeId();
        try (MongoCursor<Document> cursor = mongoCollection.find().projection(projection).iterator()){
            while (cursor.hasNext()) {
                System.out.println(cursor.next().toJson());
            }
        }
    }

    private static void visualizeById(MongoDatabase mongoDatabase) {
        MongoCollection<Document> mongoCollection = mongoDatabase.getCollection("Estados");
        Scanner scanner = new Scanner(System.in);
        System.out.println("Digite o ObjectId do documento:");
        String objectId = scanner.next();
        ObjectId objectIdFilter = new ObjectId(objectId);
        Document visualizebyId = mongoCollection.find(Filters.eq("_id", objectIdFilter)).first();
        System.out.println("O Documento referido é:");

        if (visualizebyId != null) {
            System.out.println(visualizebyId.toJson());
        } else {
            System.out.println("Documento não encontrado para o ObjectId fornecido.");
        }
    }

    private static void updateById(MongoDatabase mongoDatabase) {
        MongoCollection<Document> mongoCollection = mongoDatabase.getCollection("Estados");
        Scanner scanner = new Scanner(System.in);
        System.out.println("Digite o ObjectId do documento para atualização:");
        String objectId = scanner.next();
        ObjectId objectIdFilter = new ObjectId(objectId);
        Document filter = new Document("_id", objectIdFilter);
        Document update = new Document("$set", new Document("Região", "Sul"));
        mongoCollection.updateOne(filter, update);
        System.out.println("Atualização Concluída do Documento :"
                +mongoCollection.find(Filters.eq("_id", objectIdFilter)).first());
    }

    private static void deleteById(MongoDatabase mongoDatabase) {
        MongoCollection<Document> mongoCollection = mongoDatabase.getCollection("Estados");
        Scanner scanner = new Scanner(System.in);
        System.out.println("Digite o ObjectId do documento para exclusão:");
        String objectId = scanner.next();
        ObjectId objectIdFilter = new ObjectId(objectId);
        Document filter = new Document("_id", objectIdFilter);
        mongoCollection.deleteOne(filter);
        System.out.println("Documento Deletado");
    }
}

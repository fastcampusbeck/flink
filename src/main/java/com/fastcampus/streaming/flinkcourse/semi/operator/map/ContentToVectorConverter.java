package com.fastcampus.streaming.flinkcourse.semi.operator.map;

import com.fastcampus.streaming.flinkcourse.semi.model.News;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.ml.linalg.DenseVector;
import org.apache.flink.types.Row;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.*;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.store.MMapDirectory;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;


public class ContentToVectorConverter implements MapFunction<News, Row> {

    private static final String LUCENE_INDEX_PREFIX = "lucene-index-";
    private static final String TITLE_FIELD_NAME = "title";
    private static final String CONTENT_FIELD_NAME = "content";
    private static final List<String> KOREAN_WORDS = Arrays.asList(
            "korea", "korean", "seoul", "south korea",
            "kor", "kors", "seoul", "sk",
            "s. korea", "s. korean"
    );

    private transient MMapDirectory directory;
    private transient IndexWriter writer;

    @Override
    public Row map(News news) throws Exception {
        try {
            initializeIndex();
            indexNewsContent(news);
            return computeTFIDFVector(news);
        } finally {
            cleanupResources();
        }
    }

    private void initializeIndex() throws IOException {
        Path tempIndexPath = Files.createTempDirectory(LUCENE_INDEX_PREFIX);
        directory = new MMapDirectory(tempIndexPath);
        Analyzer analyzer = new StandardAnalyzer();
        IndexWriterConfig config = new IndexWriterConfig(analyzer);
        writer = new IndexWriter(directory, config);
    }

    private void indexNewsContent(News news) throws IOException {
        Document doc = createDocumentFromNews(news);
        writer.addDocument(doc);
        writer.commit();
    }

    private Document createDocumentFromNews(News news) {
        Document doc = new Document();
        FieldType typeWithTermVectors = createFieldTypeWithTermVectors();

        Field titleField = new Field(TITLE_FIELD_NAME, news.getTitle(), typeWithTermVectors);
        Field contentField = new Field(CONTENT_FIELD_NAME, news.getContent(), typeWithTermVectors);
        doc.add(titleField);
        doc.add(contentField);

        return doc;
    }

    private FieldType createFieldTypeWithTermVectors() {
        FieldType typeWithTermVectors = new FieldType(TextField.TYPE_STORED);
        typeWithTermVectors.setStoreTermVectors(true);
        typeWithTermVectors.setTokenized(true);
        typeWithTermVectors.setStored(true);
        return typeWithTermVectors;
    }

    private Row computeTFIDFVector(News news) throws IOException {
        List<Double> titleTFIDFValues = computeTFIDFValuesForField(news, TITLE_FIELD_NAME);
        List<Double> contentTFIDFValues = computeTFIDFValuesForField(news, CONTENT_FIELD_NAME);

        List<Double> tfidfValues = mergeAndNormalizeTFIDFValues(titleTFIDFValues, contentTFIDFValues);
        tfidfValues = pad(tfidfValues, 100);

        DenseVector denseVector = new DenseVector(tfidfValues.stream().mapToDouble(d -> d).toArray());
        return Row.of(denseVector, news.getTitle(), news.getBroadcast_date());
    }

    private List<Double> computeTFIDFValuesForField(News news, String field) throws IOException {
        List<Double> tfidfValues = new ArrayList<>();
        List<String> keywords = extractKeywords(news.getContent());

        try (DirectoryReader reader = DirectoryReader.open(directory)) {
            IndexSearcher searcher = createIndexSearcher(reader);

            for (String keyword : keywords) {
                if (!KOREAN_WORDS.contains(keyword)) {
                    tfidfValues.add(computeTFIDFValueForKeyword(searcher, keyword, field));
                }
            }
        }

        return tfidfValues;
    }

    private IndexSearcher createIndexSearcher(DirectoryReader reader) {
        IndexSearcher searcher = new IndexSearcher(reader);
        searcher.setSimilarity(new ClassicSimilarity());
        return searcher;
    }

    private double computeTFIDFValueForKeyword(IndexSearcher searcher, String keyword, String field) throws IOException {
        TermQuery query = new TermQuery(new Term(field, keyword));
        TopDocs topDocs = searcher.search(query, 1);
        return topDocs.scoreDocs.length > 0 ? topDocs.scoreDocs[0].score : 0.0;
    }

    private List<String> extractKeywords(String text) {
        return Arrays.stream(text.split(" "))
                .filter(token -> token.length() > 2)
                .collect(Collectors.toList());
    }

    private List<Double> mergeAndNormalizeTFIDFValues(List<Double> titleTFIDFValues, List<Double> contentTFIDFValues) {
        List<Double> tfidfValues = new ArrayList<>();
        tfidfValues.addAll(titleTFIDFValues);
        tfidfValues.addAll(contentTFIDFValues);
        return normalizeTFIDFValues(tfidfValues);
    }

    private List<Double> normalizeTFIDFValues(List<Double> tfidfValues) {
        double norm = computeL2Norm(tfidfValues);
        return tfidfValues.stream().map(value -> value / norm).collect(Collectors.toList());
    }

    private double computeL2Norm(List<Double> values) {
        return Math.sqrt(values.stream().mapToDouble(d -> d * d).sum());
    }

    private List<Double> pad(List<Double> tfidfValues, int desiredLength) {
        List<Double> paddedValues = new ArrayList<>(desiredLength);

        for (int i = 0; i < desiredLength; i++) {
            if (i < tfidfValues.size()) {
                paddedValues.add(tfidfValues.get(i));
            } else {
                paddedValues.add(0.0);
            }
        }

        return paddedValues;
    }

    private void cleanupResources() {
        try {
            Path pathToDelete = directory.getDirectory();
            writer.close();
            directory.close();
            deleteDirectoryRecursively(pathToDelete);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void deleteDirectoryRecursively(Path path) {
        if (Files.isDirectory(path)) {
            try (DirectoryStream<Path> entries = Files.newDirectoryStream(path)) {
                for (Path entry : entries) {
                    deleteDirectoryRecursively(entry);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        try {
            Files.delete(path);
        } catch (IOException e) {
            throw new RuntimeException("Failed to delete " + path, e);
        }
    }
}

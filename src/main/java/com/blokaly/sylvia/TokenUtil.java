package com.blokaly.sylvia;

import edu.stanford.nlp.simple.Sentence;
import org.nibor.autolink.LinkExtractor;
import org.nibor.autolink.LinkType;
import org.nibor.autolink.Span;
import org.nibor.autolink.internal.SpanImpl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;
import java.util.StringJoiner;
import java.util.function.Consumer;

public class TokenUtil {

  private static final String PUNCT = "\"#!$%&()*+,-./:;<=>?@[]^_`{|}~€$0123456789";
  private static final LinkExtractor LINK_EXTRACTOR = LinkExtractor.builder()
      .linkTypes(EnumSet.of(LinkType.URL, LinkType.WWW, LinkType.EMAIL))
      .build();

  public static String lemmas(String text) {
    String clean = clean(text);
    return toLemmaString(clean);
  }

  private static String toLemmaString(String clean) {
    Sentence sentence = new Sentence(clean);
    StringJoiner joiner = new StringJoiner(" ");
    for (String lemma : sentence.lemmas()) {
      if (isLemmaOk(lemma)) {
        joiner.add(lemma);
      }
    }
    return joiner.toString();
  }

  private static boolean isLemmaOk(String lemma) {
    if (lemma.length() <= 1) {
      return false;
    }
    if (lemma.charAt(0) == '\'') {
      return false;
    }
    return true;
  }

  private static String clean(String text) {

    StringBuilder sb = new StringBuilder();
    for (char cha : removeLinks(text).toCharArray()) {
      if (PUNCT.indexOf(cha) >= 0) {
        sb.append(' ');
      } else {
        sb.append(cha);
      }
    }
    return sb.toString();
  }

  private static String removeLinks(String input) {
    StringBuilder sb = new StringBuilder();
    Iterable<Span> spans = LINK_EXTRACTOR.extractSpans(input);
    spans.forEach(span -> {
      if (span instanceof SpanImpl) {
        sb.append(input, span.getBeginIndex(), span.getEndIndex());
      }
    });
    return sb.toString();
  };

  private static Set<String> readDict(InputStream inputStream)  {

    HashSet<String> dict = new HashSet<>();
    try (BufferedReader br = new BufferedReader(new InputStreamReader(inputStream))) {
      String line;
      while ((line = br.readLine()) != null) {
        dict.add(line.trim());
      }
    } catch (IOException ioex) {
      ioex.printStackTrace();
    }
    return dict;
  }

}

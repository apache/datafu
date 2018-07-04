// Code from https://github.com/benelog/multiline.git
// Based on Adrian Walker's blog post: http://www.adrianwalker.org/2011/12/java-multiline-string.html

package org.adrianwalker.multilinestring;

import java.util.Set;

import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.Processor;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedAnnotationTypes;
import javax.annotation.processing.SupportedSourceVersion;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.TypeElement;

@SupportedAnnotationTypes({"org.adrianwalker.multilinestring.Multiline"})

// This generates a warning with Java 8 - however, if we switch to Java 8 and use SourceVersion.RELEASE_8, it
// prevents compilation with Java 7. So we'll keep it and ignore the warning
@SupportedSourceVersion(SourceVersion.RELEASE_7)
public final class MultilineProcessor extends AbstractProcessor {
  private Processor delegator = null;
  
  @Override
  public void init(final ProcessingEnvironment procEnv) {
    super.init(procEnv);
    String envClassName = procEnv.getClass().getName();
    if (envClassName.contains("com.sun.tools")) {
      delegator = new JavacMultilineProcessor();
    } else {
      delegator = new EcjMultilineProcessor();
    }
    delegator.init(procEnv);
  }

  @Override
  public boolean process(final Set<? extends TypeElement> annotations, final RoundEnvironment roundEnv) {
    if (delegator == null ) {
      return true;
    }
    return delegator.process(annotations, roundEnv);
  }
}
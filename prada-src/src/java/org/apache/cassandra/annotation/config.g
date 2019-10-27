grammar config;

options {
    language = Java;
}

@header {
    package org.apache.cassandra.annotation;

    import java.util.HashMap;
    import java.util.HashSet;
    import java.util.ArrayList;

    import org.apache.cassandra.annotation.dataannotations.*;
    import org.apache.cassandra.exceptions.SyntaxException;
}

@members {
    private final List<String> recognitionErrors = new ArrayList<String>();

    public void displayRecognitionError(String[] tokenNames, RecognitionException e)
    {
        String hdr = getErrorHeader(e);
        String msg = getErrorMessage(e, tokenNames);
        recognitionErrors.add(hdr + " " + msg);
    }

    public void addRecognitionError(String msg)
    {
        recognitionErrors.add(msg);
    }

    public List<String> getRecognitionErrors()
    {
        return recognitionErrors;
    }

    public void throwLastRecognitionError() throws SyntaxException
    {
        if (recognitionErrors.size() > 0)
            throw new SyntaxException(recognitionErrors.get((recognitionErrors.size()-1)));
    }
}

@lexer::header {
    package org.apache.cassandra.annotation;

    import org.apache.cassandra.exceptions.SyntaxException;
}

@lexer::members {
    List<Token> tokens = new ArrayList<Token>();

    public void emit(Token token)
    {
        state.token = token;
        tokens.add(token);
    }

    public Token nextToken()
    {
        super.nextToken();
        if (tokens.size() == 0)
            return Token.EOF_TOKEN;
        return tokens.remove(0);
    }

    private List<String> recognitionErrors = new ArrayList<String>();

    public void displayRecognitionError(String[] tokenNames, RecognitionException e)
    {
        String hdr = getErrorHeader(e);
        String msg = getErrorMessage(e, tokenNames);
        recognitionErrors.add(hdr + " " + msg);
    }

    public List<String> getRecognitionErrors()
    {
        return recognitionErrors;
    }

    public void throwLastRecognitionError() throws SyntaxException
    {
        if (recognitionErrors.size() > 0)
            throw new SyntaxException(recognitionErrors.get((recognitionErrors.size()-1)));
    }
}


result returns [DataAnnotationConfig res]
    : r=config {$res=r;}
    ;

config returns [DataAnnotationConfig res]
    @init {
        HashMap<String, AbstractDataAnnotation<?>> list = new HashMap<String, AbstractDataAnnotation<?>>();
    }
    : 'REPLICATIONFACTOR' ':' repfactor=INT NEWLINE
      'REPLICATIONSTRATEGY' ':' repstrategy=REPLICATIONSTRATEGY NEWLINE
      'LOADBALANCER' ':' loadbalancer=LOADBALANCER NEWLINE
      (annotation[list])*
     {
            $res=new DataAnnotationConfig(Integer.parseInt($repfactor.text),
                                          $repstrategy.text,
                                          $loadbalancer.text,
                                          list
                                          );
     }
    ;

annotation[HashMap<String, AbstractDataAnnotation<?>> annotation]
    : boolannotation[annotation]
    | stringannotation[annotation]
    | intannotation[annotation]
    | '#' (STRING)* (NEWLINE)
    ;

boolannotation[HashMap<String, AbstractDataAnnotation<?>> annotation]
    : 'TYPE' ':' 'BOOLEAN' NEWLINE
      'NAME' ':' name=STRING NEWLINE
      'SUPPORTED' ':' val=BOOL NEWLINE
      {annotation.put($name.text, new BoolDataAnnotation($name.text, Boolean.parseBoolean($val.text)));}
    ;

stringannotation[HashMap<String, AbstractDataAnnotation<?>> annotation]
    : 'TYPE' ':' 'STRING' NEWLINE
      'NAME' ':' name=STRING NEWLINE
      'MAX'  ':' max=INT NEWLINE
      'SUPPORTED' ':' val=stringset NEWLINE
      ('COLLECTIONS' ':' colls=collections NEWLINE)?
      {annotation.put($name.text, new StringDataAnnotation($name.text, Integer.parseInt($max.text), val, colls));}
    ;

intannotation[HashMap<String, AbstractDataAnnotation<?>> annotation]
    : 'TYPE' ':' 'INTEGER' NEWLINE
      'NAME' ':' name=STRING NEWLINE
      'MAX'  ':' max=INT NEWLINE
      'COMPARATOR' ':' comp=COMPARATORS NEWLINE
      'SUPPORTED' ':' val=integerset NEWLINE
      {annotation.put($name.text, new IntegerDataAnnotation($name.text, val, IntegerDataAnnotation.IntegerComparators.valueOf($comp.text.toUpperCase()), Integer.parseInt($max.text)));}
    ;

integerset returns [HashSet<Integer> params]
  @init {
    HashSet<Integer> tempset = new HashSet<Integer>();
  }
  : val1=INT { tempset.add(Integer.parseInt($val1.text)); } ( ',' valn=INT { tempset.add(Integer.parseInt($valn.text)); } )*
    {
      $params = tempset;
    }
  ;

stringset returns [HashSet<String> params]
  @init {
    HashSet<String> tempset = new HashSet<String>();
  }
  : val1=STRING { tempset.add($val1.text); } ( ',' valn=STRING { tempset.add($valn.text); } )*
    {
      $params = tempset;
    }
  ;

collections returns [HashMap<String, HashSet<String>> collections]
  @init {
    HashMap<String, HashSet<String>> tempcollection = new HashMap<String, HashSet<String>>();
  }
  : name=STRING '=(' vals=stringset ')' {tempcollection.put($name.text, vals);} (',' name=STRING '=(' valsn=stringset ')' {tempcollection.put($name.text, valsn);})*
  {
    $collections = tempcollection;
  }
  ;


INT : ('0'..'9')+;
REPLICATIONSTRATEGY: (S I M P L E S T R A T E G Y);
LOADBALANCER: ('SimpleLoadBalancer' | 'RandomLoadBalancer' | 'LRULoadBalancer' | 'SimpleScoringLoadBalancer' | 'CounterMinLoadBalancer');
COMPARATORS: (L E S S | E Q U A L | G R E A T E R | L E S S E Q U A L | G R E A T E R E Q U A L);
BOOL: (T R U E | F A L S E);
NEWLINE: ('\n' | '\r')+;
STRING: ('A' .. 'Z' | 'a'..'z' | '-' | INT)+;
// Case-insensitive alpha characters
fragment A: ('a'|'A');
fragment B: ('b'|'B');
fragment C: ('c'|'C');
fragment D: ('d'|'D');
fragment E: ('e'|'E');
fragment F: ('f'|'F');
fragment G: ('g'|'G');
fragment H: ('h'|'H');
fragment I: ('i'|'I');
fragment J: ('j'|'J');
fragment K: ('k'|'K');
fragment L: ('l'|'L');
fragment M: ('m'|'M');
fragment N: ('n'|'N');
fragment O: ('o'|'O');
fragment P: ('p'|'P');
fragment Q: ('q'|'Q');
fragment R: ('r'|'R');
fragment S: ('s'|'S');
fragment T: ('t'|'T');
fragment U: ('u'|'U');
fragment V: ('v'|'V');
fragment W: ('w'|'W');
fragment X: ('x'|'X');
fragment Y: ('y'|'Y');
fragment Z: ('z'|'Z');

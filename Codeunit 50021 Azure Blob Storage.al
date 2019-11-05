codeunit 50021 "Azure Blob Storage"
{
    [EventSubscriber(ObjectType::Table, Database::"EDI Configuration", 'GetFilesFromAzureBlobStorage', '', false, false)]
    local procedure GetFilesFromAzureBlobStorage(var EDIFileBuffer: Record "EDI File Buffer"; var Handled: Boolean; AzureContainerURL: Text; AccessKey: Text)
    var
        ListOfAzureBlobURLS: List of [Text];
        AzureBlobURL: Text;
        EDIOutStream: OutStream;
        EDIInStream: InStream;
        FileName: Text;
        File: File;
    begin
        if AzureContainerURL = '' then
            exit;
        Handled := true;
        ListContainer(AzureContainerURL, AccessKey, ListOfAzureBlobURLS);

        foreach AzureBlobURL in ListOfAzureBlobURLS do
        begin
            EDIFileBuffer.Filename := AzureBlobURL;
            EDIFileBuffer.Insert;
            FileName := ReadAzureFile(AzureBlobURL, EDIInStream, true);

            File.Open(FileName);
            File.CreateInStream(EDIInStream);
            EDIFileBuffer.Blob.CreateoutStream(EDIOutStream);
            CopyStream(EDIOutStream, EDIInStream);
            File.Close;
            Erase(FileName);
            EDIFileBuffer.Modify;
        end;
    end;

    [EventSubscriber(ObjectType::Codeunit, Codeunit::"Azure Blob Mgt.", 'DeleteAzureBlobEvent', '', false, false)]
    local procedure DeleteAzureBlobEvent(AzureBlobURL: Text)
    var
        File: File;
    begin
        DelezeAzureFile(AzureBlobURL);
    end;

    [EventSubscriber(ObjectType::Codeunit, Codeunit::"Azure Blob Mgt.", 'UploadStreamToAzureBlobStorageEvent', '', false, false)]
    local procedure UploadStreamToAzureBlobStorageEvent(Stream: InStream; AzureBlobURL: Text)
    var
        File: File;
    begin
        SendFileToAzure(Stream, AzureBlobURL);
    end;

    [EventSubscriber(ObjectType::Codeunit, Codeunit::"Azure Blob Mgt.", 'UploadToAzureBlobStorageEvent', '', false, false)]
    local procedure UploadToAzureBlobStorageEvent(TempFileName: Text; AzureURL: Text)
    var
        File: File;
        InStream: InStream;
    begin
        File.Open(TempFileName);
        File.CreateInStream(InStream);
        SendFileToAzure(InStream, AzureURL);
        Erase(TempFileName);
    end;

    [EventSubscriber(ObjectType::Codeunit, Codeunit::"Azure Blob Mgt.", 'DownloadToServerFromAzureBlobStorageEvent', '', false, false)]
    local procedure DownloadToServerFromAzureBlobStorageEvent(var TempFileName: Text; AzureBlobURL: Text)
    var
        InStream: InStream;
    begin
        TempFileName := ReadAzureFile(AzureBlobURL, InStream, true);
    end;

    [EventSubscriber(ObjectType::Codeunit, Codeunit::"Copy File to Other Domain", 'CopyToPublicBlobStorage', '', false, false)]
    local procedure CopyToPublicBlobStorage(FromAzureURL: Text; VAR ToAzureURL: Text)
    var
        InStream: InStream;
        FileInStream: InStream;
        TempFileName: Text;
        File: File;
    begin
        TempFileName := ReadAzureFile(FromAzureURL, InStream, true);

        File.Open(TempFileName);
        File.CreateInStream(FileInStream);

        SendFileToAzure(FileInStream, ToAzureURL);
    end;

    [EventSubscriber(ObjectType::Codeunit, Codeunit::"Azure Blob Mgt.", 'DownloadToClientFromAzureBlobStorageEvent', '', false, false)]
    local procedure DownloadToClientFromAzureBlobStorageEvent(var TempFileName: Text; AzureBlobURL: Text)
    var
        InStream: InStream;
    begin
        TempFileName := ReadAzureFile(AzureBlobURL, InStream, true);
    end;

    procedure SendFileToAzure(Istr: InStream; AzureBlobURL: Text): Boolean
    var
        Content: HttpContent;
        Headers: HttpHeaders;
        Client: HttpClient;
        ResponseMessage: HttpResponseMessage;
        ResponseText: text;
    begin
        Content.WriteFrom(Istr);
        Content.GetHeaders(Headers);
        Headers.Remove('x-ms-blob-type');
        Headers.Add('x-ms-blob-type', 'BlockBlob');
        Headers.Remove('x-ms-blob-content-type');
        if AzureBlobURL.Contains('pdf') then
            Headers.Add('x-ms-blob-content-type', 'application/pdf');
        if AzureBlobURL.Contains('csv') then
            Headers.Add('x-ms-blob-content-type', 'text/csv');

        if not Client.Put(AzureBlobURL, Content, ResponseMessage) then begin
            ResponseMessage.Content().ReadAs(ResponseText);
            error('The web service returned an error message:\' +
                      'Status code: %1' +
                      'Description: %2',
                      ResponseMessage.HttpStatusCode,
                      ResponseText);
        end;
        exit(true);
    end;

    procedure ReadAzureFile(AzureBlobURL: Text; var ResponseText: InStream; SaveAsFile: Boolean): Text;
    var
        Client: HttpClient;
        ResponseMessage: HttpResponseMessage;
        ErrorResponse: Text;
    begin
        if not Client.Get(AzureBlobURL, ResponseMessage) then
            Error('The call to the web service failed.');

        ResponseMessage.Content().ReadAs(ResponseText);

        if not ResponseMessage.IsSuccessStatusCode then begin
            ResponseMessage.Content().ReadAs(ErrorResponse);
            error('The web service returned an error message:\' +
                  'Status code: %1' +
                  'Description: %2',
                  ResponseMessage.HttpStatusCode,
                   ErrorResponse);

        end;
        if SaveAsFile then
            exit(SaveAsFile2(ResponseText));
    end;

    procedure DelezeAzureFile(AzureBlobURL: Text);
    var
        Client: HttpClient;
        ResponseMessage: HttpResponseMessage;
        ErrorResponse: Text;
    begin
        if not Client.Delete(AzureBlobURL, ResponseMessage) then
            Error('The call to the web service failed.');
    end;

    local procedure SaveAsFile2(Value: InStream) TempFileName: Text
    var
        FileMgt: Codeunit "File Management";
        TempBlob: Record TempBlob;
        OutStream: OutStream;
        InStream: InStream;
        File: File;
    begin
        TempFileName := FileMgt.ServerTempFileName('pdf');
        TempBlob.blob.CreateOutStream(OutStream);
        CopyStream(OutStream, Value);
        TempBlob.FromBase64String(TempBlob.ToBase64String);
        TempBlob.Blob.CreateInStream(InStream);
        File.WriteMode(true);
        File.Create(TempFileName);
        File.CreateOutStream(OutStream);
        CopyStream(OutStream, InStream);
        file.Close;
    end;

    local procedure ListContainer(AzureContainerURL: Text; AccessKey: Text; var Blobs: List of [Text])
    var
        Client: HttpClient;
        ResponseMessage: HttpResponseMessage;
        ResultContent: HttpContent;
        ResponseText: XmlDocument;
        InStream: InStream;
        XMLBuffer: Record "XML Buffer" temporary;
        OutStream: OutStream;
        TempBlob: Record TempBlob temporary;
    begin
        AzureContainerURL := AzureContainerURL.Replace('\', '/').TrimStart('/') + '?restype=container&comp=list';

        if not Client.Get(AzureContainerURL, ResponseMessage) then
            Error('The call to the web service failed.');

        ResultContent := ResponseMessage.Content;

        if not ResponseMessage.IsSuccessStatusCode then
            error('The web service returned an error message:\' +
                  'Status code: %1' +
                  'Description: %2',
                  ResponseMessage.HttpStatusCode,
                   ResponseText);

        ResultContent.ReadAs(InStream);

        XmlDocument.ReadFrom(InStream, ResponseText);
        TempBlob.Blob.CreateOutStream(OutStream);
        ResponseText.WriteTo(OutStream);
        XMLBuffer.Load(OutStream);
        XMLBuffer.SetRange(XMLBuffer.Type, XMLBuffer.Type::Element);
        XMLBuffer.FindSet;
        repeat
            if XMLBuffer.Name.ToUpper = 'URL' then
            blobs.Add(XMLBuffer.Value);
        until XMLBuffer.next = 0;

    end;
}